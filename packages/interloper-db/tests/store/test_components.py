"""Tests for the generic component store (``interloper_db.store.components``)."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import ClassVar
from uuid import uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, InUseError, NotFoundError
from interloper_assets.demo.source import DemoSource
from sqlalchemy import Engine, create_engine
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.store import Store
from interloper_db.store.components import ComponentStore
from interloper_db.store.status import ComponentStatus

_ORG = uuid4()


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store over the in-memory database (no catalog needed for these).

    Returns:
        A store with an empty catalog, reading and writing the fixture database.
    """
    return Store(catalog=il.Catalog(components={}))


def _relations(session: Session, src_id, type: str | None = None) -> list[ComponentRelation]:
    statement = select(ComponentRelation).where(ComponentRelation.src_id == src_id)
    if type:
        statement = statement.where(ComponentRelation.type == type)
    return list(session.exec(statement).all())


class TestCrud:
    """Generic CRUD semantics shared by every kind."""

    def test_secret_kinds_encrypt_config_into_data(self, component_db: Engine):
        store = Store(catalog=il.Catalog(components={}), encrypt=lambda b: b[::-1], decrypt=lambda b: b[::-1])
        row = store.components.create(_ORG, kind="connection", key="conn", name="C", config={"token": "s3cret"})
        assert row.config is None
        assert row.encrypted is True
        assert store.components.decode_config(row) == {"token": "s3cret"}

    def test_secret_kinds_fail_closed_without_cipher(self, store: Store):
        with pytest.raises(ConfigError):
            store.components.create(_ORG, kind="connection", key="conn", config={"token": "s3cret"})

    def test_children_rejected_for_childless_kinds(self, store: Store):
        with pytest.raises(ConfigError):
            store.components.create(_ORG, kind="destination", key="dest", children=["a"])

    def test_unknown_child_keys_rejected(self, component_db: Engine):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        with pytest.raises(ConfigError, match=r"declares no asset\(s\) \['typo'\]"):
            store.components.create(_ORG, kind="source", key="demo_source", children=["a", "typo"])

    def test_delete_refuses_source_owned_assets(self, store: Store, component_db: Engine):
        job = store.components.create(_ORG, kind="job", key="cron_job")  # any parentable stand-in row
        with Session(component_db) as session:
            child = Component(org_id=_ORG, kind="asset", key="a", parent_id=job.id)
            session.add(child)
            session.commit()
            child_id = child.id
        with pytest.raises(ValueError):
            store.components.delete(child_id)

    def test_delete_source_removes_child_rows(self, component_db: Engine):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        source = store.components.create(_ORG, kind="source", key="demo_source")
        assert source.children

        store.components.delete(source.id)

        # The DB cascade must delete the children — a regression here leaves
        # them behind as orphaned parentless asset rows instead.
        with Session(component_db) as session:
            assert session.exec(select(Component).where(Component.kind == "asset")).all() == []

    def test_delete_cascades_outbound_relations(self, store: Store):
        job = store.components.create(_ORG, kind="job", key="cron_job", name="J")
        asset = store.components.create(_ORG, kind="asset", key="a")
        store.relations.add(job.id, type="target", dst_id=asset.id)

        store.components.delete(job.id)
        assert store.relations.list_all(_ORG) == []

    def test_list_filters_org_and_kinds(self, store: Store):
        store.components.create(_ORG, kind="destination", key="mine")
        store.components.create(_ORG, kind="asset", key="other_kind")
        store.components.create(uuid4(), kind="destination", key="other_org")

        rows = store.components.list_all(_ORG, kinds=["destination"])
        assert [row.key for row in rows] == ["mine"]
        assert {row.key for row in store.components.list_all(_ORG)} == {"mine", "other_kind"}

    def test_get_component_checks_kind(self, store: Store):
        dest = store.components.create(_ORG, kind="destination", key="dest")
        assert store.components.get(dest.id, kind="destination").id == dest.id
        with pytest.raises(NotFoundError):
            store.components.get(dest.id, kind="asset")


class TestDeleteInUseGuard:
    """A relation destination cannot be deleted while external referrers exist."""

    def _connection(self, store: Store) -> Component:
        return store.components.create(_ORG, kind="connection", key="conn", name="Conn", config={}, encrypted=False)

    def test_bound_connection_blocks_delete_and_names_referrer(self, store: Store):
        conn = self._connection(store)
        asset = store.components.create(_ORG, kind="asset", key="a", name="A", relations={"resource": [(conn.id, "c")]})

        with pytest.raises(InUseError) as excinfo:
            store.components.delete(conn.id)
        assert excinfo.value.referrers == [{"id": str(asset.id), "kind": "asset", "key": "a", "name": "A"}]
        assert "in use by A" in str(excinfo.value)

    def test_delete_succeeds_after_unbinding(self, store: Store):
        conn = self._connection(store)
        asset = store.components.create(_ORG, kind="asset", key="a", relations={"resource": [(conn.id, "c")]})

        store.relations.remove(asset.id, type="resource", dst_id=conn.id)
        store.components.delete(conn.id)
        with pytest.raises(NotFoundError):
            store.components.get(conn.id)

    def test_job_target_detaches(self, store: Store):
        asset = store.components.create(_ORG, kind="asset", key="a")
        job = store.components.create(
            _ORG, kind="job", key="cron_job", name="J", relations={"target": [(asset.id, "")]}
        )

        store.components.delete(asset.id)
        assert store.components.get(job.id).id == job.id
        assert store.relations.list_all(_ORG) == []

    def test_hook_watch_detaches(self, store: Store):
        asset = store.components.create(_ORG, kind="asset", key="a")
        hook = store.components.create(
            _ORG, kind="hook", key="webhook", name="H", relations={"watch": [(asset.id, "")]}
        )

        store.components.delete(asset.id)
        assert store.components.get(hook.id).id == hook.id
        assert store.relations.list_all(_ORG) == []

    def test_blocking_relation_wins_over_detaching(self, store: Store):
        conn = self._connection(store)
        asset = store.components.create(_ORG, kind="asset", key="a", name="A", relations={"resource": [(conn.id, "c")]})
        store.components.create(_ORG, kind="job", key="cron_job", relations={"target": [(asset.id, "")]})

        # The asset both consumes the connection (blocks its deletion) and is
        # a job target (detachable) — deleting the asset succeeds, deleting
        # the connection does not.
        with pytest.raises(InUseError):
            store.components.delete(conn.id)
        store.components.delete(asset.id)

    def test_referrer_through_child_reports_parent(self, store: Store, component_db: Engine):
        conn = self._connection(store)
        parent = store.components.create(_ORG, kind="job", key="cron_job", name="P")  # parentable stand-in
        with Session(component_db) as session:
            child = Component(org_id=_ORG, kind="asset", key="a", parent_id=parent.id)
            session.add(child)
            session.commit()
            child_id = child.id
        store.relations.add(child_id, type="resource", dst_id=conn.id, slot="c")

        with pytest.raises(InUseError) as excinfo:
            store.components.delete(conn.id)
        assert [r["id"] for r in excinfo.value.referrers] == [str(parent.id)]

    def test_intra_subtree_relations_do_not_block(self, store: Store, component_db: Engine):
        parent = store.components.create(_ORG, kind="job", key="cron_job", name="P")  # parentable stand-in
        with Session(component_db) as session:
            a = Component(org_id=_ORG, kind="asset", key="a", parent_id=parent.id)
            b = Component(org_id=_ORG, kind="asset", key="b", parent_id=parent.id)
            session.add(a)
            session.add(b)
            session.commit()
            a_id, b_id = a.id, b.id
        store.relations.add(b_id, type="dependency", dst_id=a_id, slot="a")

        store.components.delete(parent.id)
        with pytest.raises(NotFoundError):
            store.components.get(parent.id)


class GuardUpstream(il.Asset):
    """Upstream asset for the delete-guard dependency tests."""


class GuardRequired(il.Asset):
    """Asset with a required dependency on ``guard_upstream``."""

    requires: ClassVar[dict[str, str]] = {"up": "guard_upstream"}


class GuardOptional(il.Asset):
    """Asset with an optional dependency on ``guard_upstream``."""

    optional_requires: ClassVar[dict[str, str]] = {"up": "guard_upstream"}


class TestDependencyDeleteSemantics:
    """Required dependency slots block deletion; optional slots detach."""

    @pytest.fixture
    def dep_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([GuardUpstream, GuardRequired, GuardOptional]))

    def test_required_dependency_blocks(self, dep_store: Store):
        up = dep_store.components.create(_ORG, kind="asset", key="guard_upstream", name="Up")
        down = dep_store.components.create(
            _ORG, kind="asset", key="guard_required", relations={"dependency": [(up.id, "up")]}
        )

        with pytest.raises(InUseError) as excinfo:
            dep_store.components.delete(up.id)
        assert [r["id"] for r in excinfo.value.referrers] == [str(down.id)]

    def test_optional_dependency_detaches(self, dep_store: Store):
        up = dep_store.components.create(_ORG, kind="asset", key="guard_upstream", name="Up")
        down = dep_store.components.create(
            _ORG, kind="asset", key="guard_optional", relations={"dependency": [(up.id, "up")]}
        )

        dep_store.components.delete(up.id)
        assert dep_store.components.get(down.id).id == down.id
        assert dep_store.relations.list_all(_ORG) == []


class WireUpSource(il.Source):
    """Upstream source for the cross-source dependency tests."""

    class Rows(il.Asset):
        """Upstream asset (key ``rows``)."""


class WireDownSource(il.Source):
    """Downstream source whose asset requires ``wire_up_source.rows``."""

    class Consumer(il.Asset):
        """Asset with a required cross-source dependency."""

        requires: ClassVar[dict[str, str]] = {"rows": "wire_up_source.rows"}


class WireDownOptionalSource(il.Source):
    """Downstream source whose asset optionally consumes ``wire_up_source.rows``."""

    class Reader(il.Asset):
        """Asset with an optional cross-source dependency."""

        optional_requires: ClassVar[dict[str, str]] = {"rows": "wire_up_source.rows"}


def _child(source: Component, key: str) -> Component:
    return next(child for child in source.children if child.key == key)


class TestIntraSourceWiring:
    """Intra-source dependency edges are derived idempotently over the full child set."""

    @pytest.fixture
    def demo_store(self, component_db: Engine) -> Store:
        from interloper_assets.demo.source import DemoSource

        return Store(catalog=il.Catalog.from_assets([DemoSource]))

    def test_full_dag_wired_on_create(self, demo_store: Store):
        demo_store.components.create(_ORG, kind="source", key="demo_source")
        edges = demo_store.relations.list_all(_ORG, type="dependency")
        assert len(edges) == 6  # b,c,d -> a and e -> b,c,d

    def test_children_enabled_later_get_inbound_edges(self, demo_store: Store):
        source = demo_store.components.create(_ORG, kind="source", key="demo_source", children=["b", "e"])
        assert [r.slot for r in demo_store.relations.list_all(_ORG, type="dependency")] == ["b"]  # only e -> b

        updated = demo_store.components.update(source.id, children=["a", "b", "e"])
        edges = demo_store.relations.list_all(_ORG, type="dependency")
        by_slot = {r.slot: (r.src_id, r.dst_id) for r in edges}
        assert set(by_slot) == {"a", "b"}
        assert by_slot["a"] == (_child(updated, "b").id, _child(updated, "a").id)

    def test_wiring_is_idempotent(self, demo_store: Store):
        source = demo_store.components.create(_ORG, kind="source", key="demo_source")
        demo_store.components.update(source.id, name="renamed")
        demo_store.components.update(source.id, children=["a", "b", "c", "d", "e"])
        assert len(demo_store.relations.list_all(_ORG, type="dependency")) == 6

    def test_update_without_children_leaves_child_set_untouched(self, demo_store: Store):
        source = demo_store.components.create(_ORG, kind="source", key="demo_source", children=["b", "e"])
        updated = demo_store.components.update(source.id, name="renamed")
        assert {child.key for child in updated.children} == {"b", "e"}


class TestChildRemovalGuard:
    """Narrowing a source's child set honors the delete guard's semantics."""

    @pytest.fixture
    def wire_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([WireUpSource, WireDownSource, WireDownOptionalSource]))

    def test_removing_child_with_required_external_dep_blocked(self, wire_store: Store):
        up = wire_store.components.create(_ORG, kind="source", key="wire_up_source", name="Up")
        down = wire_store.components.create(_ORG, kind="source", key="wire_down_source", name="Down")
        wire_store.relations.add(
            _child(down, "consumer").id, type="dependency", dst_id=_child(up, "rows").id, slot="rows"
        )

        with pytest.raises(InUseError) as excinfo:
            wire_store.components.update(up.id, children=[])
        assert [r["id"] for r in excinfo.value.referrers] == [str(down.id)]
        assert wire_store.relations.list_all(_ORG, type="dependency") != []

    def test_removing_child_with_optional_external_dep_detaches(self, wire_store: Store):
        up = wire_store.components.create(_ORG, kind="source", key="wire_up_source")
        down = wire_store.components.create(_ORG, kind="source", key="wire_down_optional_source")
        wire_store.relations.add(
            _child(down, "reader").id, type="dependency", dst_id=_child(up, "rows").id, slot="rows"
        )

        updated = wire_store.components.update(up.id, children=[])
        assert updated.children == []
        assert wire_store.relations.list_all(_ORG, type="dependency") == []

    def test_intra_source_reshape_not_blocked(self, component_db: Engine):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        source = store.components.create(_ORG, kind="source", key="demo_source")
        updated = store.components.update(source.id, children=["a"])
        assert [child.key for child in updated.children] == ["a"]
        assert store.relations.list_all(_ORG, type="dependency") == []


class DiscriminatedSource(il.Source):
    """Source class whose instances are discriminated by ``account_id``."""

    account_id: str = il.InputField(default="", discriminator=True)

    class DiscriminatedRows(il.Asset):
        """Asset whose table name carries the instance discriminator."""


class TestSourceCollisionGuard:
    """A second source instance may not target the same physical tables."""

    @pytest.fixture
    def guard_store(self, component_db: Engine) -> Store:
        from interloper_assets.demo.source import DemoSource

        return Store(catalog=il.Catalog.from_assets([DemoSource, DiscriminatedSource]))

    def test_same_alias_rejected(self, guard_store: Store):
        guard_store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})

    def test_distinct_alias_allowed(self, guard_store: Store):
        guard_store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        second = guard_store.components.create(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "2"}
        )
        assert second.id is not None

    def test_alias_compared_after_sanitization(self, guard_store: Store):
        guard_store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "act-1"})
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.components.create(
                _ORG, kind="source", key="discriminated_source", config={"account_id": "ACT_1"}
            )

    def test_undiscriminated_source_needs_distinct_dataset(self, guard_store: Store):
        guard_store.components.create(_ORG, kind="source", key="demo_source")
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.components.create(_ORG, kind="source", key="demo_source")
        second = guard_store.components.create(_ORG, kind="source", key="demo_source", config={"dataset": "other"})
        assert second.id is not None

    def test_update_into_collision_rejected(self, guard_store: Store):
        guard_store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        second = guard_store.components.create(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "2"}
        )
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.components.update(second.id, config={"account_id": "1"})

    def test_other_org_does_not_collide(self, guard_store: Store):
        guard_store.components.create(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        other = guard_store.components.create(
            uuid4(), kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        assert other.id is not None


class TestDerivedNames:
    """A blank ``components.name`` defaults to the instance's derived display name."""

    @pytest.fixture
    def name_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([DiscriminatedSource]))

    def test_blank_name_defaults_to_instance_name(self, name_store: Store):
        row = name_store.components.create(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        assert row.name == "1"

    def test_explicit_name_wins(self, name_store: Store):
        row = name_store.components.create(
            _ORG, kind="source", key="discriminated_source", name="Mine", config={"account_id": "1"}
        )
        assert row.name == "Mine"

    def test_default_name_follows_config_change(self, name_store: Store):
        row = name_store.components.create(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        updated = name_store.components.update(row.id, config={"account_id": "2"})
        assert updated.name == "2"

    def test_customized_name_untouched_by_config_change(self, name_store: Store):
        row = name_store.components.create(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        name_store.components.update(row.id, name="Mine")
        updated = name_store.components.update(row.id, config={"account_id": "2"})
        assert updated.name == "Mine"

    def test_unresolvable_key_leaves_name_blank(self, store: Store):
        row = store.components.create(_ORG, kind="destination", key="ghost")
        assert row.name is None


class TestTelemetry:
    """Hydration is traced where it happens."""

    def test_load_emits_a_span_per_hydration(self, component_db: Engine, span_exporter):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        source = store.components.create(_ORG, kind="source", key="demo_source")

        store.components.load(source.id)

        spans = [s for s in span_exporter.get_finished_spans() if s.name == "interloper.store.load"]
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["interloper.target.id"] == str(source.id)


class TestQuotaGates:
    """Capacity quotas gate source creation and the child-asset set size."""

    def _store(self, **limits: int | None) -> Store:
        from types import SimpleNamespace

        from interloper_assets.demo.source import DemoSource

        return Store(catalog=il.Catalog.from_assets([DemoSource]), quota_defaults=SimpleNamespace(**limits))

    def test_source_limit_blocks_creation(self, component_db: Engine):
        from interloper.errors import QuotaExceededError

        store = self._store(max_sources=1)
        store.components.create(_ORG, kind="source", key="demo_source", children=["a"])
        with pytest.raises(QuotaExceededError) as excinfo:
            store.components.create(_ORG, kind="source", key="demo_source", children=["a"])
        assert excinfo.value.quota == "max_sources"
        assert (excinfo.value.limit, excinfo.value.used) == (1, 1)

    def test_source_limit_org_override_wins(self, component_db: Engine):
        from interloper.errors import QuotaExceededError

        from interloper_db.models import Quota

        store = self._store(max_sources=1)
        with Session(component_db) as session:
            session.add(Quota(org_id=_ORG, key="max_sources", limit=2))
            session.commit()
        store.components.create(_ORG, kind="source", key="demo_source", children=["a"], config={"dataset": "one"})
        store.components.create(_ORG, kind="source", key="demo_source", children=["a"], config={"dataset": "two"})
        with pytest.raises(QuotaExceededError):
            store.components.create(_ORG, kind="source", key="demo_source", children=["a"], config={"dataset": "3"})

    def test_asset_limit_blocks_large_child_set(self, component_db: Engine):
        from interloper.errors import QuotaExceededError

        store = self._store(max_assets_per_source=2)
        source = store.components.create(_ORG, kind="source", key="demo_source", children=["a", "b"])
        with pytest.raises(QuotaExceededError) as excinfo:
            store.components.update(source.id, children=["a", "b", "c"])
        assert excinfo.value.quota == "max_assets_per_source"
        # Creation with the full catalog set (children=None -> 5 assets) is also gated.
        with pytest.raises(QuotaExceededError):
            store.components.create(_ORG, kind="source", key="demo_source", config={"dataset": "full"})
        # Shrinking or staying within the limit is fine.
        assert {c.key for c in store.components.update(source.id, children=["a"]).children} == {"a"}

    def test_unconfigured_quotas_gate_nothing(self, component_db: Engine):
        store = self._store()
        for dataset in ("one", "two", "three"):
            store.components.create(_ORG, kind="source", key="demo_source", config={"dataset": dataset})


class TestStatus:
    """A row's catalog status, resolved through its parent for a source-owned asset."""

    @pytest.fixture
    def demo_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([DemoSource]))

    def test_live_source_is_ok(self, demo_store: Store):
        row = demo_store.components.create(_ORG, kind="source", key=DemoSource.key)
        assert demo_store.components.status(row) is ComponentStatus.OK

    def test_owned_asset_resolves_through_its_parent(self, demo_store: Store):
        # The child rows nested under a fetched source carry no loaded parent:
        # resolving one has to reach a row away, not lazy-load a detached edge.
        source = demo_store.components.create(_ORG, kind="source", key=DemoSource.key)
        child = _child(demo_store.components.get(source.id), "a")
        assert demo_store.components.status(child) is ComponentStatus.OK

    def test_parent_key_spares_the_lookup(self, demo_store: Store):
        source = demo_store.components.create(_ORG, kind="source", key=DemoSource.key)
        child = _child(demo_store.components.get(source.id), "a")
        assert demo_store.components.status(child, parent_key=DemoSource.key) is ComponentStatus.OK
        # The hint is taken at face value; a wrong one resolves against it.
        assert demo_store.components.status(child, parent_key="gone_source") is ComponentStatus.MISSING

    def test_key_outside_the_catalog_is_missing(self, store: Store):
        row = Component(org_id=_ORG, kind="source", key="gone_source")
        assert store.components.status(row) is ComponentStatus.MISSING


# -- Resource encoding ---------------------------------------------------------


def _encoder(encrypt: Callable[[bytes], bytes] | None) -> ComponentStore:
    """The component facet of a store carrying only the cipher under test.

    Args:
        encrypt: Cipher the store encrypts resources with, or None for an
            instance with no encryption key configured.

    Returns:
        The facet whose ``_encode_data`` decides encryption. Its engine is
        never connected to: encoding is pure.
    """
    store = Store(catalog=il.Catalog(components={}), engine=create_engine("sqlite://"), encrypt=encrypt)
    return store.components


def _fake_encrypt(data: bytes) -> bytes:
    return b"ENC:" + data


class TestResourceEncoding:
    """``_encode_data`` is the single place that decides whether a blob is encrypted."""

    def test_default_encrypts_when_key_is_configured(self) -> None:
        raw, encrypted = _encoder(_fake_encrypt)._encode_data({"a": 1}, None)
        assert encrypted is True
        assert raw == b"ENC:" + json.dumps({"a": 1}).encode()

    def test_default_without_key_raises(self) -> None:
        # Fail closed: the default must never silently store a resource in plaintext.
        with pytest.raises(ConfigError):
            _encoder(None)._encode_data({"a": 1}, None)

    def test_explicit_true_without_key_raises(self) -> None:
        with pytest.raises(ConfigError):
            _encoder(None)._encode_data({"a": 1}, True)

    def test_explicit_false_stays_plaintext_even_with_key(self) -> None:
        raw, encrypted = _encoder(_fake_encrypt)._encode_data({"a": 1}, False)
        assert encrypted is False
        assert raw == json.dumps({"a": 1}).encode()

    def test_explicit_false_without_key_stays_plaintext(self) -> None:
        # Opting out explicitly still works without a key (for non-secret resources).
        raw, encrypted = _encoder(None)._encode_data({"a": 1}, False)
        assert encrypted is False
        assert raw == json.dumps({"a": 1}).encode()
