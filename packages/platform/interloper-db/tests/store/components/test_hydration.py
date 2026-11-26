"""Round-trip tests: generic store writes → generic hydration → live framework objects.

These exercise the full pipeline against a real (SQLite) database using real
catalog classes: create rows through the generic store surface, hydrate them,
and assert on the reconstructed framework instances.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pydantic
import pytest
from sqlalchemy import Engine, event
from sqlmodel import Session, select

from interloper_db import ComponentDriftError, HydrationError
from interloper_db.models import Component, ComponentRelation
from interloper_db.store import ComponentQuery, RelationQuery, Store
from interloper_db.store.components import Hydrator

_ORG = uuid4()
_DAILY = il.TimePartitionConfig(column="date", allow_window=False)


class DemoSource(il.Source):
    """A daily source whose assets form the ``a -> b,c,d -> e`` DAG."""

    hello: str = il.InputField(default="world")

    @il.asset(partitioning=_DAILY)
    def a(self) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def b(self, a: il.Upstream) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def c(self, a: il.Upstream) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def d(self, a: il.Upstream) -> list[dict]:
        return []

    @il.asset(partitioning=_DAILY)
    def e(self, b: il.Upstream, c: il.Upstream, d: il.Upstream) -> list[dict]:
        return []


@il.asset(partitioning=_DAILY)
def demo_asset() -> list[dict]:
    return []


@il.asset(partitioning=_DAILY)
def follower_asset(demo_asset: il.Upstream) -> list[dict]:
    return []


class ShopConnection(il.Connection):
    """Connection the shop source binds."""


class Warehouse(il.MemoryDestination):
    """Destination the test sources write to."""


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


class Account(il.Source):
    """Discriminated source, so several accounts coexist in one organisation."""

    account_id: str = il.InputField(default="", discriminator=True)

    class Stats(il.Asset):
        """The asset ``report.summary`` reads."""

        def data(self, context: il.ExecutionContext) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.

            Returns:
                No rows.
            """
            return []


class Report(il.Source):
    """Source whose single-valued upstream reads ``account.stats``."""

    class Summary(il.Asset):
        """Asset reading exactly one account's ``stats``."""

        stats = il.Relation("asset", "account.stats")

        def data(self, context: il.ExecutionContext, stats: il.Upstream) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.
                stats: The upstream leg read from ``account.stats``.

            Returns:
                No rows.
            """
            return []


class CampaignsA(il.Source):
    """First provider of a ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaigns."""

        def data(self, context: il.ExecutionContext) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.

            Returns:
                No rows.
            """
            return []


class CampaignsB(il.Source):
    """Second provider of a ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaigns."""

        def data(self, context: il.ExecutionContext) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.

            Returns:
                No rows.
            """
            return []


class Rollup(il.Source):
    """Source whose ``totals`` reads every source's ``campaigns`` and the shop's ``orders``."""

    class Totals(il.Asset):
        """Totals over campaigns and orders."""

        campaigns: list[il.Asset] = il.Relation("asset", "*.campaigns", many=True, optional=True)
        orders: il.Asset | None = il.Relation("asset", "shop.orders", optional=True)

        def data(self, context: il.ExecutionContext) -> list[dict]:  # pragma: no cover
            """Never materialized by these tests.

            Args:
                context: The execution context of the materialization.

            Returns:
                No rows.
            """
            return []


class Tally(il.Asset):
    """Parentless asset reading every source's ``campaigns``."""

    campaigns: list[il.Asset] = il.Relation("asset", "*.campaigns", many=True, optional=True)

    def data(self, context: il.ExecutionContext) -> list[dict]:  # pragma: no cover
        """Never materialized by these tests.

        Args:
            context: The execution context of the materialization.

        Returns:
            No rows.
        """
        return []


class FakeLinker(il.Component):
    """Test-only kind whose vocabulary the hydrator has never seen."""

    links: list[il.Component] = il.Relation(["source", "fake_linker"], many=True, optional=True)


il.KINDS.register(FakeLinker.kind, FakeLinker.anchor())

_CATALOG = il.Catalog.from_assets(
    [
        DemoSource,
        demo_asset,
        follower_asset,
        Shop,
        Finance,
        Account,
        Report,
        Warehouse,
        CampaignsA,
        CampaignsB,
        Rollup,
        Tally,
    ]
)


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store over the in-memory database with the tests' catalog.

    Returns:
        A store carrying the demo and shop/finance classes, reading and
        writing the fixture database.
    """
    return Store(catalog=_CATALOG)


@pytest.fixture
def shop_connection(store: Store) -> Component:
    """The connection row a ``shop`` source must bind.

    Returns:
        The created connection component.
    """
    return store.components.create(_ORG, kind="connection", key="shop_connection", config={}, encrypted=False)


@contextmanager
def _statements(engine: Engine) -> Generator[list[str]]:
    sent: list[str] = []

    def record(_connection, _cursor, statement, *_) -> None:
        sent.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        yield sent
    finally:
        event.remove(engine, "before_cursor_execute", record)


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


class TestRelationTargets:
    """How a relation's targets reach the hydrated component."""

    def test_a_parentless_target_is_hydrated_and_bound(self, store: Store):
        connection = store.components.create(_ORG, kind="connection", key="shop_connection", config={}, encrypted=False)
        warehouse = store.components.create(_ORG, kind="destination", key="warehouse")
        shop = store.components.create(
            _ORG,
            kind="source",
            key="shop",
            relations={"connection": [connection.id], "destinations": [warehouse.id]},
        )

        hydrated = store.components.load(shop.id)

        assert isinstance(hydrated, Shop)
        assert hydrated.connection.id == str(connection.id)
        assert [target.id for target in hydrated.destinations] == [str(warehouse.id)]

    def test_a_target_reached_twice_hydrates_once(self, store: Store, shop_connection: Component):
        warehouse = store.components.create(_ORG, kind="destination", key="warehouse")
        shop = store.components.create(
            _ORG,
            kind="source",
            key="shop",
            relations={"connection": [shop_connection.id], "destinations": [warehouse.id]},
        )
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

        hydrated = store.components.load(job.id)

        assert isinstance(hydrated, il.Job)
        first, second = (target.destinations[0] for target in hydrated.targets)
        assert first is second
        assert first.id == str(warehouse.id)

    def test_a_relation_name_the_class_does_not_declare_is_an_actionable_error(
        self, store: Store, shop_connection: Component, component_db: Engine
    ):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        warehouse = store.components.create(_ORG, kind="destination", key="warehouse")
        with Session(component_db) as session:
            session.add(
                ComponentRelation(
                    from_id=shop.id,
                    relation="mystery",
                    to_id=warehouse.id,
                    org_id=_ORG,
                    from_kind="source",
                    to_kind="destination",
                )
            )
            session.commit()

        with pytest.raises(HydrationError, match="has 'mystery' relations its class does not declare"):
            store.components.load(shop.id)

    def test_two_rows_under_a_single_valued_relation_is_an_actionable_error(
        self, store: Store, shop_connection: Component, component_db: Engine
    ):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        # Hand-inserted: the store's own writes repoint a single-valued name
        # instead of accumulating, so only a rogue writer produces this row.
        rogue = store.components.create(_ORG, kind="connection", key="shop_connection", config={}, encrypted=False)
        with Session(component_db) as session:
            session.add(
                ComponentRelation(
                    from_id=shop.id,
                    relation="connection",
                    to_id=rogue.id,
                    org_id=_ORG,
                    from_kind="source",
                    to_kind="connection",
                )
            )
            session.commit()

        with pytest.raises(HydrationError, match="holds 2 rows under single-valued relation 'connection'"):
            store.components.load(shop.id)


class TestSourceRoundTrip:
    """Sources with child assets, intra-source deps, and overrides."""

    def test_create_source_creates_children_and_intra_deps(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        assert db_source.kind == "source"
        assert sorted(child.key for child in db_source.children) == ["a", "b", "c", "d", "e"]

        deps = store.relations.list(_ORG, RelationQuery(from_kind="asset", to_kind="asset", limit=None)).items
        by_child: dict[str, set[tuple[str, str]]] = {}
        children_by_id = {child.id: child.key for child in db_source.children}
        for relation in deps:
            by_child.setdefault(children_by_id[relation.from_id], set()).add(
                (relation.relation, children_by_id[relation.to_id])
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
        store.components.update(child.id, config={"enabled": False})

        asset = store.components.load(child.id)
        assert isinstance(asset, il.Asset)
        assert asset.key == "a"
        assert asset.enabled is False

    def test_children_selection_drops_rows_and_relations(self, store: Store):
        db_source = store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        store.components.update(db_source.id, children=["a", "b"])

        refreshed = store.components.get(db_source.id, kind="source")
        assert sorted(child.key for child in refreshed.children) == ["a", "b"]
        # e (and its dependency relations) are gone; b keeps its dep on a.
        remaining = store.relations.list(_ORG, RelationQuery(from_kind="asset", to_kind="asset", limit=None)).items
        assert [relation.relation for relation in remaining] == ["a"]


class TestAutomaticUpstreams:
    """An upstream relation holding no row is bound to the organisation's matches on load, never written."""

    def test_load_binds_unbound_upstreams_to_the_organisations_matches(
        self, store: Store, shop_connection: Component, component_db: Engine
    ):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        finance = store.components.create(_ORG, kind="source", key="finance")

        hydrated = store.components.load(finance.id)

        orders = _asset(hydrated, "revenue").bound("orders")
        assert isinstance(orders, il.Asset) and orders.id == str(_child(shop, "orders").id)
        with Session(component_db) as session:
            assert session.exec(select(ComponentRelation).where(ComponentRelation.from_kind == "asset")).all() == []

    def test_load_leaves_an_unmatched_required_relation_unbound(self, store: Store):
        finance = store.components.create(_ORG, kind="source", key="finance")

        hydrated = store.components.load(finance.id)

        assert _asset(hydrated, "revenue").bound("orders") is None

    def test_load_refuses_a_single_valued_relation_with_two_matches(self, store: Store):
        for account in ("a", "b"):
            store.components.create(_ORG, kind="source", key="account", config={"account_id": account})
        report = store.components.create(_ORG, kind="source", key="report")

        with pytest.raises(HydrationError, match="'stats' matches 2 assets"):
            store.components.load(report.id)

    def test_following_siblings_sends_the_statements_an_idle_source_sends(self, store: Store, component_db: Engine):
        following = store.components.create(_ORG, kind="source", key="demo_source", config={"dataset": "first"})
        idle = store.components.create(
            _ORG, kind="source", key="demo_source", config={"dataset": "second", "enabled": False}
        )

        with _statements(component_db) as idle_sent:
            store.components.load(idle.id)
        with _statements(component_db) as following_sent:
            hydrated = store.components.load(following.id)

        assert _asset(hydrated, "b").bound("a") is _asset(hydrated, "a")
        assert following_sent == idle_sent

    def test_load_leaves_a_disabled_assets_upstreams_alone(self, store: Store):
        for account in ("a", "b"):
            store.components.create(_ORG, kind="source", key="account", config={"account_id": account})
        report = store.components.create(_ORG, kind="source", key="report", config={"enabled": False})

        hydrated = store.components.load(report.id)

        assert _asset(hydrated, "summary").bound("stats") is None

    def test_a_parentless_asset_follows_the_parentless_asset_it_names(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="demo_asset")
        follower = store.components.create(_ORG, kind="asset", key="follower_asset")

        hydrated = store.components.load(follower.id)

        bound = hydrated.bound("demo_asset")
        assert isinstance(bound, il.Asset) and bound.id == str(upstream.id)

    def test_siblings_bind_without_rows(self, store: Store):
        demo = store.components.create(_ORG, kind="source", key="demo_source")

        hydrated = store.components.load(demo.id)

        assert _asset(hydrated, "b").bound("a") is _asset(hydrated, "a")


@pytest.fixture
def rollup_org(store: Store, shop_connection: Component) -> SimpleNamespace:
    """An organisation where the rollup could read both providers' campaigns and the shop's orders.

    Returns:
        The rows: the rollup and its ``totals``, both providers and their
        ``campaigns``, the shop.
    """
    provider_a = store.components.create(_ORG, kind="source", key="campaigns_a")
    provider_b = store.components.create(_ORG, kind="source", key="campaigns_b")
    shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
    rollup = store.components.create(_ORG, kind="source", key="rollup")
    return SimpleNamespace(
        rollup=rollup,
        totals=_child(rollup, "totals"),
        shop=shop,
        provider_a=provider_a,
        provider_b=provider_b,
        a_campaigns=_child(provider_a, "campaigns"),
    )


class TestUpstreamSources:
    """Upstream sources narrow what hydration binds, and travel with the component, so a run's DAG narrows alike."""

    def test_a_relation_follows_only_its_upstream_sources(self, store: Store, rollup_org: SimpleNamespace):
        store.relations.add(rollup_org.rollup.id, relation=il.UPSTREAM, to_id=rollup_org.provider_a.id)

        totals = _asset(store.components.load(rollup_org.rollup.id), "totals")

        campaigns = totals.bound("campaigns")
        assert isinstance(campaigns, list)
        assert [asset.id for asset in campaigns] == [str(rollup_org.a_campaigns.id)]
        assert totals.bound("orders") is None

    def test_the_upstream_sources_are_bound(self, store: Store, rollup_org: SimpleNamespace):
        store.relations.add(rollup_org.rollup.id, relation=il.UPSTREAM, to_id=rollup_org.provider_a.id)

        rollup = store.components.load(rollup_org.rollup.id)

        assert [source.id for source in _asset(rollup, "totals").upstream_sources() or []] == [
            str(rollup_org.provider_a.id)
        ]
        campaigns, upstream = _asset(rollup, "totals").bound("campaigns"), rollup.bound(il.UPSTREAM)
        assert isinstance(campaigns, list) and isinstance(upstream, list)
        assert campaigns[0].parent is upstream[0]

    def test_a_parentless_assets_own_upstream_sources_narrow_what_it_follows(
        self, store: Store, rollup_org: SimpleNamespace
    ):
        tally = store.components.create(_ORG, kind="asset", key="tally")
        store.relations.add(tally.id, relation=il.UPSTREAM, to_id=rollup_org.provider_a.id)

        campaigns = store.components.load(tally.id).bound("campaigns")

        assert isinstance(campaigns, list)
        assert [asset.id for asset in campaigns] == [str(rollup_org.a_campaigns.id)]

    def test_a_runs_dag_never_widens_a_relation_its_upstream_sources_left_empty(
        self, store: Store, rollup_org: SimpleNamespace
    ):
        store.relations.add(rollup_org.rollup.id, relation=il.UPSTREAM, to_id=rollup_org.provider_a.id)
        rollup = store.components.load(rollup_org.rollup.id)
        shop = store.components.load(rollup_org.shop.id)
        assert isinstance(rollup, il.Source) and isinstance(shop, il.Source)

        dag = il.DAG(*rollup.operations(), *shop.operations())
        restored = il.DAG.from_spec(dag.to_spec())

        for graph in (dag, restored):
            totals = next(operation for operation in graph.operations if operation.key == "totals")
            assert totals.bound("orders") is None

    def test_upstream_sources_add_no_statement_to_a_load(
        self, store: Store, rollup_org: SimpleNamespace, component_db: Engine
    ):
        with _statements(component_db) as following:
            store.components.load(rollup_org.rollup.id)
        for provider in (rollup_org.provider_a, rollup_org.provider_b, rollup_org.shop):
            store.relations.add(rollup_org.rollup.id, relation=il.UPSTREAM, to_id=provider.id)
        with _statements(component_db) as narrowed:
            store.components.load(rollup_org.rollup.id)

        assert len(narrowed) <= len(following)

    def test_an_upstream_source_hydrates_once(
        self, store: Store, rollup_org: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
    ):
        store.relations.add(rollup_org.rollup.id, relation=il.UPSTREAM, to_id=rollup_org.provider_a.id)
        original_build = Hydrator._build
        built: list[UUID] = []

        def spy(self: Hydrator, db_component: Component, cache: Any, chain: Any) -> il.Component:
            built.append(db_component.id)
            return original_build(self, db_component, cache, chain)

        monkeypatch.setattr(Hydrator, "_build", spy)

        store.components.load(rollup_org.rollup.id)

        assert built.count(rollup_org.provider_a.id) == 1
        assert rollup_org.provider_b.id not in built


class TestCrossSourceUpstream:
    """A reference no document carries is resolved through the store."""

    def test_load_resolves_cross_source_upstream_through_store(self, store: Store, shop_connection: Component):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        store.relations.add(_child(finance, "revenue").id, relation="orders", to_id=shop_orders.id)

        hydrated = store.components.load(finance.id)

        orders = _asset(hydrated, "revenue").bound("orders")
        assert isinstance(orders, il.Asset)
        assert orders.id == str(shop_orders.id)
        assert orders.parent is not None
        assert orders.parent.key == "shop"

    def test_an_owned_asset_referencing_another_source_loads(self, store: Store, shop_connection: Component):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        revenue_row = _child(finance, "revenue")
        store.relations.add(revenue_row.id, relation="orders", to_id=shop_orders.id)

        revenue = store.components.load(revenue_row.id)

        assert isinstance(revenue, il.Asset)
        bound = revenue.bound("orders")
        assert isinstance(bound, il.Asset)
        assert bound.id == str(shop_orders.id)

    def test_a_shared_upstream_reached_twice_hydrates_once(
        self, store: Store, shop_connection: Component, monkeypatch: pytest.MonkeyPatch
    ):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        store.relations.add(_child(finance, "revenue").id, relation="orders", to_id=shop_orders.id)
        store.relations.add(_child(finance, "cogs").id, relation="orders", to_id=shop_orders.id)

        original_build = Hydrator._build
        shop_hydrations: list[UUID] = []

        def spy(self: Hydrator, db_component: Component, cache: Any, chain: Any) -> il.Component:
            if db_component.key == "shop":
                shop_hydrations.append(db_component.id)
            return original_build(self, db_component, cache, chain)

        monkeypatch.setattr(Hydrator, "_build", spy)

        hydrated = store.components.load(finance.id)

        revenue_orders = _asset(hydrated, "revenue").bound("orders")
        cogs_orders = _asset(hydrated, "cogs").bound("orders")
        assert isinstance(revenue_orders, il.Asset)
        assert isinstance(cogs_orders, il.Asset)
        assert revenue_orders is cogs_orders
        assert revenue_orders.parent is cogs_orders.parent
        assert shop_hydrations == [shop.id]

    def test_assets_of_two_sources_referencing_each_other_bind_both_ways(
        self, store: Store, shop_connection: Component
    ):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        finance = store.components.create(_ORG, kind="source", key="finance")
        shop_orders = _child(shop, "orders")
        revenue = _child(finance, "revenue")
        store.relations.add(revenue.id, relation="orders", to_id=shop_orders.id)
        store.relations.add(shop_orders.id, relation="back", to_id=revenue.id)

        hydrated = store.components.load(shop.id)

        orders = _asset(hydrated, "orders")
        back = orders.bound("back")
        assert isinstance(back, il.Asset)
        assert back.id == str(revenue.id)
        assert back.bound("orders") is orders

    def test_a_cycle_through_parentless_components_raises_with_its_trail(self, store: Store, component_db: Engine):
        store._catalog.components["fake_linker"] = FakeLinker.definition()
        first = store.components.create(_ORG, kind="fake_linker", key="fake_linker", name="First")
        second = store.components.create(_ORG, kind="fake_linker", key="fake_linker", name="Second")
        with Session(component_db) as session:
            for src, dst in ((first, second), (second, first)):
                session.add(
                    ComponentRelation(
                        from_id=src.id,
                        relation="links",
                        to_id=dst.id,
                        org_id=_ORG,
                        from_kind="fake_linker",
                        to_kind="fake_linker",
                    )
                )
            session.commit()

        with pytest.raises(HydrationError, match="Reference cycle while hydrating: fake_linker -> fake_linker"):
            store.components.load(first.id)

    def test_a_reference_to_a_drifted_component_raises_component_drift_error(
        self, store: Store, shop_connection: Component
    ):
        shop = store.components.create(_ORG, kind="source", key="shop", relations={"connection": [shop_connection.id]})
        finance = store.components.create(_ORG, kind="source", key="finance")
        store.relations.add(_child(finance, "revenue").id, relation="orders", to_id=_child(shop, "orders").id)

        reader = Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset, Finance, Warehouse]))

        with pytest.raises(ComponentDriftError, match="shop"):
            reader.components.load(finance.id)


class TestStandaloneAsset:
    """Standalone assets hydrate directly through the generic builder."""

    def test_create_and_load(self, store: Store):
        db_asset = store.components.create(_ORG, kind="asset", key="demo_asset", config={"enabled": False})
        asset = store.components.load(db_asset.id)
        assert isinstance(asset, il.Asset)
        assert asset.key == "demo_asset"
        assert asset.id == str(db_asset.id)
        assert asset.enabled is False


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
        assert [relation.to_id for relation in db_job.out_relations] == [db_source.id]

        jobs = store.components.list(_ORG, ComponentQuery(kind=["job"], roots_only=False, limit=None)).items
        assert [job.id for job in jobs] == [db_job.id]

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

            def __init__(self, **init: Any) -> None:
                super().__init__(token="s3cret-value")

        monkeypatch.setattr(il.Component, "resolve_key", classmethod(lambda cls, key, catalog=None: Probe))
        with pytest.raises(HydrationError) as exc_info:
            store.components.load(db_source.id)

        message = str(exc_info.value)
        assert "Failed to hydrate source 'demo_source'" in message
        assert "app_secret: Field required" in message
        assert "s3cret-value" not in message


class TestJobTargets:
    """A job whose target drifted cannot be hydrated."""

    def test_a_drifted_target_fails_closed(self, component_db: Engine):
        writer = Store(catalog=il.Catalog.from_assets([DemoSource]))
        source = writer.components.create(_ORG, kind="source", key="demo_source")
        job = writer.components.create(_ORG, kind="job", key="cron_job", relations={"targets": [source.id]})
        reader = Store(catalog=il.Catalog.from_assets([]))

        with pytest.raises(ComponentDriftError, match=r"target source 'demo_source' .* does not resolve"):
            reader.components.load(job.id)
