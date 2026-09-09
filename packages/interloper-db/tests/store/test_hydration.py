"""Round-trip tests: generic store writes → generic hydration → live framework objects.

These exercise the full pipeline against a real (SQLite) database using real
catalog classes: create rows through the generic store surface, hydrate
through the one generic spec builder, and assert on the reconstructed
framework instances.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pydantic
import pytest
from interloper.errors import ComponentDriftError, HydrationError
from interloper.serializable import Spec
from interloper_assets.demo.source import DemoSource, demo_asset
from sqlalchemy import Engine
from sqlmodel import Session

from interloper_db.models import Component, ComponentRelation
from interloper_db.store import Store
from interloper_db.store.hydration import Hydrator

_ORG = uuid4()


class ShopConnection(il.Connection):
    """Connection the shop source binds."""


class Warehouse(il.Destination):
    """Destination the test sources write to."""

    def read(self, context: Any) -> Any:  # pragma: no cover
        """Never read by these tests.

        Args:
            context: The IO context of the read.

        Returns:
            Nothing; the destination is a persistence fixture only.
        """
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        """Never written by these tests.

        Args:
            context: The IO context of the write.
            data: The payload that would be written.
        """


class Shop(il.Source):
    """Upstream source: a bound connection and one asset other sources read."""

    connection: ShopConnection

    class Orders(il.Asset):
        """The asset the finance source reads by qualified key."""

        back = il.Relation("asset", "finance.revenue", optional=True)

        def data(self, context: il.ExecutionContext) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.

            Returns:
                No rows.
            """
            return []


class Finance(il.Source):
    """Downstream source whose assets both read ``shop.orders``."""

    class Revenue(il.Asset):
        """Asset with a cross-source upstream."""

        orders = il.Relation("asset", "shop.orders")

        def data(self, context: il.ExecutionContext, orders: il.Upstream) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.
                orders: The upstream leg read from ``shop.orders``.

            Returns:
                No rows.
            """
            return []

    class Cogs(il.Asset):
        """A second asset reading the same cross-source upstream as ``Revenue``."""

        orders = il.Relation("asset", "shop.orders")

        def data(self, context: il.ExecutionContext, orders: il.Upstream) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.
                orders: The upstream leg read from ``shop.orders``.

            Returns:
                No rows.
            """
            return []


_CATALOG = il.Catalog.from_assets([DemoSource, demo_asset, Shop, Finance, Warehouse])


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store over the in-memory database with the tests' catalog.

    Returns:
        A store carrying the demo and shop/finance classes, reading and
        writing the fixture database.
    """
    return Store(catalog=_CATALOG)


def _child(source: Component, key: str) -> Component:
    """Pick one child row of a source row by asset key.

    Args:
        source: The source row whose children are searched.
        key: The child asset's catalog key.

    Returns:
        The matching child row.
    """
    return next(child for child in source.children if child.key == key)


def _asset(source: il.Component, key: str) -> il.Asset:
    """Pick one asset instance of a hydrated source by key.

    Args:
        source: The hydrated source.
        key: The asset's catalog key.

    Returns:
        The matching asset instance.
    """
    assert isinstance(source, il.Source)
    return next(asset for asset in source.assets if asset.key == key)


def _init(store: Store, component_id: UUID) -> dict[str, Any]:
    """Build the init payload of one row, in a session of its own.

    Args:
        store: The store whose hydrator builds the payload.
        component_id: Id of the row to build.

    Returns:
        The row's init payload.
    """
    with Session(store.engine) as session:
        row = session.get(Component, component_id)
        assert row is not None
        return store.components._hydrator._build_init(session, row)


class TestBuildInit:
    """The one rule that decides how a relation target is written out."""

    def test_parentless_target_nests(self, store: Store):
        connection = store.components.create(_ORG, kind="connection", key="shop_connection", config={}, encrypted=False)
        warehouse = store.components.create(_ORG, kind="destination", key="warehouse")
        shop = store.components.create(
            _ORG,
            kind="source",
            key="shop",
            relations={"connection": [connection.id], "destinations": [warehouse.id]},
        )

        init = _init(store, shop.id)

        assert init["connection"]["id"] == str(connection.id)
        assert init["connection"]["path"].endswith("ShopConnection")
        assert [target["id"] for target in init["destinations"]] == [str(warehouse.id)]

    def test_asset_target_is_a_reference(self, store: Store):
        shop = store.components.create(_ORG, kind="source", key="shop")
        finance = store.components.create(_ORG, kind="source", key="finance")
        store.relations.add(_child(finance, "revenue").id, name="orders", dst_id=_child(shop, "orders").id)

        init = _init(store, finance.id)

        assert init["assets"]["revenue"]["orders"] == Spec.reference(str(_child(shop, "orders").id))

    def test_a_source_owned_asset_carries_its_sibling_relations(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="demo_source")

        init = _init(store, source.id)

        assert init["assets"]["e"]["b"] == Spec.reference(str(_child(source, "b").id))
        assert init["assets"]["b"]["a"] == Spec.reference(str(_child(source, "a").id))

    def test_a_target_reached_twice_is_written_out_once_then_referenced(self, store: Store):
        warehouse = store.components.create(_ORG, kind="destination", key="warehouse")
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"destinations": [warehouse.id]})
        finance = store.components.create(
            _ORG, kind="source", key="finance", relations={"destinations": [warehouse.id]}
        )
        job = store.components.create(
            _ORG,
            kind="job",
            key="cron_job",
            config={"cron": "0 6 * * *"},
            relations={"targets": [shop.id, finance.id]},
        )

        init = _init(store, job.id)

        emitted = [target["init"]["destinations"][0] for target in init["targets"]]
        assert [Spec.is_reference(value) for value in emitted] == [False, True]
        assert emitted[0]["id"] == str(warehouse.id)
        assert emitted[1] == Spec.reference(str(warehouse.id))

    def test_a_relation_name_the_class_does_not_declare_is_an_actionable_error(
        self, store: Store, component_db: Engine
    ):
        shop = store.components.create(_ORG, kind="source", key="shop")
        warehouse = store.components.create(_ORG, kind="destination", key="warehouse")
        with Session(component_db) as session:
            session.add(
                ComponentRelation(
                    src_id=shop.id,
                    name="mystery",
                    dst_id=warehouse.id,
                    org_id=_ORG,
                    src_kind="source",
                    dst_kind="destination",
                )
            )
            session.commit()

        with pytest.raises(HydrationError, match="has 'mystery' relations its class does not declare"):
            _init(store, shop.id)

    def test_two_rows_under_a_single_valued_relation_is_an_actionable_error(
        self, store: Store, component_db: Engine
    ):
        shop = store.components.create(_ORG, kind="source", key="shop")
        # Hand-inserted: the store's own writes repoint a single-valued name
        # instead of accumulating, so only a rogue writer produces this row.
        connections = [
            store.components.create(_ORG, kind="connection", key="shop_connection", config={}, encrypted=False)
            for _ in range(2)
        ]
        with Session(component_db) as session:
            for connection in connections:
                session.add(
                    ComponentRelation(
                        src_id=shop.id,
                        name="connection",
                        dst_id=connection.id,
                        org_id=_ORG,
                        src_kind="source",
                        dst_kind="connection",
                    )
                )
            session.commit()

        with pytest.raises(HydrationError, match="holds 2 rows under single-valued relation 'connection'"):
            _init(store, shop.id)


class TestSourceRoundTrip:
    """Sources with child assets, intra-source deps, and overrides."""

    def test_create_source_creates_children_and_intra_deps(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        assert db_source.kind == "source"
        assert sorted(child.key for child in db_source.children) == ["a", "b", "c", "d", "e"]

        deps = store.relations.list_all(_ORG, src_kind="asset", dst_kind="asset")
        by_child: dict[str, set[tuple[str, str]]] = {}
        children_by_id = {child.id: child.key for child in db_source.children}
        for relation in deps:
            by_child.setdefault(children_by_id[relation.src_id], set()).add(
                (relation.name, children_by_id[relation.dst_id])
            )
        assert by_child == {
            "b": {("a", "a")},
            "c": {("a", "a")},
            "d": {("a", "a")},
            "e": {("b", "b"), ("c", "c"), ("d", "d")},
        }

    def test_load_hydrates_with_stable_ids_and_deps(self, store: Store):
        db_source = store.components.create(
            _ORG, kind="source", key="demo_source", name="Demo", config={"hello": "there"}
        )
        source = store.components.load(db_source.id)

        assert isinstance(source, DemoSource)
        assert source.id == str(db_source.id)
        assert source.hello == "there"

        rows_by_key = {child.key: str(child.id) for child in db_source.children}
        assets_by_key = {asset.key: asset for asset in source.assets}
        assert {key: asset.id for key, asset in assets_by_key.items()} == rows_by_key
        e = assets_by_key["e"]
        assert {name: getattr(e, name).id for name in ("b", "c", "d")} == {name: rows_by_key[name] for name in "bcd"}

    def test_source_owned_asset_loads_through_its_parent(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        child = _child(db_source, "a")
        store.components.update(child.id, config={"materializable": False})

        asset = store.components.load(child.id)
        assert isinstance(asset, il.Asset)
        assert asset.key == "a"
        assert asset.materializable is False

    def test_children_selection_drops_rows_and_relations(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.update(db_source.id, children=["a", "b"])

        refreshed = store.components.get(db_source.id, kind="source")
        assert sorted(child.key for child in refreshed.children) == ["a", "b"]
        # e (and its dependency relations) are gone; b keeps its dep on a.
        remaining = store.relations.list_all(_ORG, src_kind="asset", dst_kind="asset")
        assert [relation.name for relation in remaining] == ["a"]


class TestCrossSourceUpstream:
    """A reference no document carries is resolved through the store."""

    def test_load_resolves_cross_source_upstream_through_store(self, store: Store):
        shop = store.components.create(_ORG, kind="source", key="shop")
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        store.relations.add(_child(finance, "revenue").id, name="orders", dst_id=shop_orders.id)

        hydrated = store.components.load(finance.id)

        orders = _asset(hydrated, "revenue").bound("orders")
        assert isinstance(orders, il.Asset)
        assert orders.id == str(shop_orders.id)
        assert orders.parent is not None
        assert orders.parent.key == "shop"

    def test_an_owned_asset_referencing_another_source_loads(self, store: Store):
        shop = store.components.create(_ORG, kind="source", key="shop")
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        revenue_row = _child(finance, "revenue")
        store.relations.add(revenue_row.id, name="orders", dst_id=shop_orders.id)

        revenue = store.components.load(revenue_row.id)

        assert isinstance(revenue, il.Asset)
        assert revenue.orders.id == str(shop_orders.id)  # ty: ignore[unresolved-attribute]

    def test_a_shared_upstream_reached_twice_hydrates_once(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        shop = store.components.create(_ORG, kind="source", key="shop")
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        store.relations.add(_child(finance, "revenue").id, name="orders", dst_id=shop_orders.id)
        store.relations.add(_child(finance, "cogs").id, name="orders", dst_id=shop_orders.id)

        original_build = Hydrator.build_component_spec
        shop_hydrations: list[UUID] = []

        def spy(self: Hydrator, session: Session, db_component: Component, *, seen: set[str] | None = None) -> Spec:
            if db_component.key == "shop":
                shop_hydrations.append(db_component.id)
            return original_build(self, session, db_component, seen=seen)

        monkeypatch.setattr(Hydrator, "build_component_spec", spy)

        hydrated = store.components.load(finance.id)

        revenue_orders = _asset(hydrated, "revenue").bound("orders")
        cogs_orders = _asset(hydrated, "cogs").bound("orders")
        assert isinstance(revenue_orders, il.Asset)
        assert isinstance(cogs_orders, il.Asset)
        assert revenue_orders is cogs_orders
        assert revenue_orders.parent is cogs_orders.parent
        assert shop_hydrations == [shop.id]

    def test_a_reference_cycle_across_sources_raises_with_its_trail(self, store: Store):
        shop = store.components.create(_ORG, kind="source", key="shop")
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        revenue = _child(finance, "revenue")
        store.relations.add(revenue.id, name="orders", dst_id=shop_orders.id)
        store.relations.add(shop_orders.id, name="back", dst_id=revenue.id)

        with pytest.raises(HydrationError, match="Reference cycle while hydrating: shop"):
            store.components.load(shop.id)

    def test_a_reference_to_a_drifted_component_raises_component_drift_error(self, store: Store):
        shop = store.components.create(_ORG, kind="source", key="shop")
        finance = store.components.create(_ORG, kind="source", key="finance")
        store.relations.add(_child(finance, "revenue").id, name="orders", dst_id=_child(shop, "orders").id)

        reader = Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset, Finance, Warehouse]))

        with pytest.raises(ComponentDriftError, match="shop"):
            reader.components.load(finance.id)


class TestStandaloneAsset:
    """Standalone assets hydrate directly through the generic builder."""

    def test_create_and_load(self, store: Store):
        db_asset = store.components.create(_ORG, kind="asset", key="demo_asset", config={"materializable": False})
        asset = store.components.load(db_asset.id)
        assert isinstance(asset, il.Asset)
        assert asset.key == "demo_asset"
        assert asset.id == str(db_asset.id)
        assert asset.materializable is False


class TestJobRoundTrip:
    """Jobs persist as components with target relations and hydrate to core Jobs."""

    def test_create_and_read_back(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        db_job = store.components.create(
            _ORG,
            kind="job",
            key="cron_job",
            name="Demo Daily",
            config={"cron": "0 6 * * *", "tags": ["daily"], "enabled": True},
            relations={"targets": [db_source.id]},
        )
        assert db_job.name == "Demo Daily"
        assert db_job.config == {"cron": "0 6 * * *", "tags": ["daily"], "enabled": True}
        assert db_job.state is None
        assert [relation.dst_id for relation in db_job.out_relations] == [db_source.id]

        assert [job.id for job in store.components.list_all(_ORG, kinds=["job"])] == [db_job.id]

    def test_load_hydrates_targets(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        db_asset = store.components.create(_ORG, kind="asset", key="demo_asset")
        db_job = store.components.create(
            _ORG,
            kind="job",
            key="cron_job",
            name="Demo Daily",
            config={"cron": "0 6 * * *"},
            relations={"targets": [db_source.id, db_asset.id]},
        )

        job = store.components.load(db_job.id)
        assert isinstance(job, il.CronJob)
        assert job.cron == "0 6 * * *"
        assert {target.key for target in job.targets} == {"demo_source", "demo_asset"}
        assert {asset.key for asset in il.DAG(*job.targets).operations} == {"a", "b", "c", "d", "e", "demo_asset"}

    def test_load_hydrates_a_target_that_is_a_source_owned_asset(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        owned_asset = _child(db_source, "a")
        db_job = store.components.create(
            _ORG,
            kind="job",
            key="cron_job",
            name="Demo Daily",
            config={"cron": "0 6 * * *"},
            relations={"targets": [owned_asset.id]},
        )

        job = store.components.load(db_job.id)
        assert isinstance(job, il.CronJob)
        target = next(iter(job.targets))
        assert target.key == "a"
        assert target.parent is not None
        assert target.parent.key == "demo_source"

    def test_update_preserves_state_but_drops_the_cached_schedule(self, store: Store):
        db_job = store.components.create(_ORG, kind="job", key="cron_job", name="Job", config={"cron": "0 6 * * *"})

        # Simulate the scheduler's targeted state write.
        from interloper_db.engine import get_engine

        with Session(get_engine()) as session:
            row = session.get(Component, db_job.id)
            assert row is not None
            row.state = {"next_run_at": "2026-07-07T06:00:00+00:00", "last_run_at": "2026-07-06T06:00:00+00:00"}
            session.add(row)
            session.commit()

        updated = store.components.update(db_job.id, name="Renamed", config={"cron": "0 7 * * *"})
        assert updated.name == "Renamed"
        assert updated.state == {"next_run_at": None, "last_run_at": "2026-07-06T06:00:00+00:00"}


class FakeLinker(il.Component):
    """Test-only kind whose vocabulary the hydrator has never seen."""

    links: list[il.Component] = il.Relation("source", many=True, optional=True)


il.KINDS.register(FakeLinker.kind, FakeLinker.anchor())


class TestOpenVocabulary:
    """A novel kind + relation name persists and hydrates with no per-kind code."""

    def test_a_custom_relation_name_round_trips(self, store: Store):
        store._catalog.components["fake_linker"] = FakeLinker.definition()

        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        db_linker = store.components.create(
            _ORG, kind="fake_linker", key="fake_linker", name="L", relations={"links": [db_source.id]}
        )

        linker = store.components.load(db_linker.id)
        assert isinstance(linker, FakeLinker)
        assert [linked.key for linked in linker.links] == ["demo_source"]
        assert linker.links[0].id == str(db_source.id)


class TestHydrationErrorSanitisation:
    """Reconstruction failures never echo the (decrypted) payload into the message."""

    def test_load_failure_message_omits_input_values(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")

        class Probe(pydantic.BaseModel):
            app_secret: str

        def raise_validation_error(spec, catalog=None, *, resolve=None):
            Probe.model_validate({"token": "s3cret-value"})

        monkeypatch.setattr(il.Component, "from_spec", raise_validation_error)
        with pytest.raises(HydrationError) as exc_info:
            store.components.load(db_source.id)

        message = str(exc_info.value)
        assert "Failed to hydrate source 'demo_source'" in message
        assert "app_secret: Field required" in message
        assert "s3cret-value" not in message


class TestDecodeData:
    """The stored ``data`` payload, decrypted for sensitive kinds."""

    def test_no_data_decodes_to_an_empty_dict(self, store: Store):
        row = store.components.create(_ORG, kind="source", key="demo_source")

        assert store.components._hydrator.decode_data(row) == {}

    def test_an_encrypted_row_without_a_cipher_is_an_actionable_error(self, component_db: Engine):
        writer = Store(catalog=_CATALOG, encrypt=lambda b: b, decrypt=lambda b: b)
        row = writer.components.create(_ORG, kind="connection", key="shop_connection", config={"token": "s3cret"})
        row.data = row.data or b"payload"
        row.encrypted = True
        reader = Store(catalog=_CATALOG)

        with pytest.raises(HydrationError, match="INTERLOPER_ENCRYPTION_KEY"):
            reader.components._hydrator.decode_data(row)


class TestRelationsByName:
    """The grouped relation lookup used while building a spec."""

    def test_a_row_with_no_id_has_no_relations(self, store: Store):
        with Session(store.engine) as session:
            assert store.components._hydrator._relations_by_name(session, None) == {}


class TestResolvePath:
    """A spec's import path comes from the catalog entry."""

    def test_a_drifted_key_is_an_actionable_error(self, component_db: Engine):
        from interloper.errors import CatalogKeyError

        writer = Store(catalog=_CATALOG)
        row = writer.components.create(_ORG, kind="source", key="demo_source")
        reader = Store(catalog=il.Catalog(components={}))

        with Session(component_db) as session:
            db_row = session.get(type(row), row.id)
            assert db_row is not None
            with pytest.raises(CatalogKeyError, match="Unknown source key: demo_source"):
                reader.components._hydrator._resolve_path(session, db_row)
