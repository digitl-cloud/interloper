"""Tests for the relation read/write layer (``interloper_db.store.relations``)."""

from __future__ import annotations

from typing import ClassVar
from uuid import uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, NotFoundError
from interloper_assets.demo.source import DemoSource
from interloper_assets.facebook_ads.source import FacebookAds
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.store import Store

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


def _child(source: Component, key: str) -> Component:
    return next(child for child in source.children if child.key == key)


class GuardUpstream(il.Asset):
    """Upstream asset for the unbind-guard dependency tests."""


class GuardRequired(il.Asset):
    """Asset with a required dependency on ``guard_upstream``."""

    requires: ClassVar[dict[str, str]] = {"up": "guard_upstream"}


class GuardOptional(il.Asset):
    """Asset with an optional dependency on ``guard_upstream``."""

    optional_requires: ClassVar[dict[str, str]] = {"up": "guard_upstream"}


class WireUpSource(il.Source):
    """Upstream source for the cross-source dependency tests."""

    class Rows(il.Asset):
        """Upstream asset (key ``rows``)."""


class WireDownSource(il.Source):
    """Downstream source whose asset requires ``wire_up_source.rows``."""

    class Consumer(il.Asset):
        """Asset with a required cross-source dependency."""

        requires: ClassVar[dict[str, str]] = {"rows": "wire_up_source.rows"}


class TestRelations:
    """Replace semantics, vocabulary validation, denormalized stamping."""

    def test_sync_stamps_org_and_kinds(self, store: Store, component_db: Engine):
        dest = store.components.create(_ORG, kind="destination", key="dest")
        asset = store.components.create(_ORG, kind="asset", key="a", relations={"destination": [(dest.id, "")]})

        with Session(component_db) as session:
            (relation,) = _relations(session, asset.id)
            assert (relation.type, relation.slot, relation.dst_id) == ("destination", "", dest.id)
            assert (relation.org_id, relation.src_kind, relation.dst_kind) == (_ORG, "asset", "destination")

    def test_update_replaces_only_the_given_type(self, store: Store, component_db: Engine):
        dest = store.components.create(_ORG, kind="destination", key="dest")
        first = store.components.create(_ORG, kind="connection", key="first", config={}, encrypted=False)
        second = store.components.create(_ORG, kind="connection", key="second", config={}, encrypted=False)
        asset = store.components.create(
            _ORG,
            kind="asset",
            key="a",
            relations={"destination": [(dest.id, "")], "resource": [(first.id, "conn")]},
        )

        store.components.update(asset.id, relations={"resource": [(second.id, "conn")]})

        with Session(component_db) as session:
            assert [r.dst_id for r in _relations(session, asset.id, "resource")] == [second.id]
            assert len(_relations(session, asset.id, "destination")) == 1

    def test_empty_list_clears_the_type(self, store: Store, component_db: Engine):
        dest = store.components.create(_ORG, kind="destination", key="dest")
        asset = store.components.create(_ORG, kind="asset", key="a", relations={"destination": [(dest.id, "")]})
        store.components.update(asset.id, relations={"destination": []})
        with Session(component_db) as session:
            assert _relations(session, asset.id) == []

    def test_rejects_types_outside_the_kind_vocabulary(self, store: Store):
        dest = store.components.create(_ORG, kind="destination", key="dest")
        with pytest.raises(ConfigError):
            store.components.create(_ORG, kind="destination", key="d2", relations={"target": [(dest.id, "")]})

    def test_rejects_missing_and_cross_org_destinations(self, store: Store):
        other = store.components.create(uuid4(), kind="destination", key="dest")
        with pytest.raises(NotFoundError):
            store.components.create(_ORG, kind="asset", key="a", relations={"destination": [(uuid4(), "")]})
        with pytest.raises(NotFoundError):
            store.components.create(_ORG, kind="asset", key="b", relations={"destination": [(other.id, "")]})

    def test_add_and_remove_relation(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="a")
        downstream = store.components.create(_ORG, kind="asset", key="b")

        relation = store.relations.add(downstream.id, type="dependency", dst_id=upstream.id, slot="a")
        assert (relation.src_id, relation.dst_id, relation.slot) == (downstream.id, upstream.id, "a")
        assert len(store.relations.list_all(_ORG, type="dependency")) == 1

        store.relations.remove(downstream.id, type="dependency", dst_id=upstream.id)
        assert store.relations.list_all(_ORG) == []


class TestRelationKindEnforcement:
    """Relation writes are checked against the vocabulary's allowed kinds."""

    @pytest.fixture
    def demo_store(self, component_db: Engine) -> Store:
        from interloper_assets.demo.source import DemoSource, demo_asset

        return Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset]))

    def test_class_vocabulary_governs_writes(self, demo_store: Store):
        db_job = demo_store.components.create(_ORG, kind="job", key="cron_job", name="J")
        # TriggerHook declares `target`; WebhookHook does not.
        ok = demo_store.components.create(
            _ORG, kind="hook", key="trigger_hook", name="T", relations={"target": [(db_job.id, "")]}
        )
        assert ok.id is not None
        with pytest.raises(ConfigError, match="'webhook_hook'.*declare no 'target' relations"):
            demo_store.components.create(
                _ORG, kind="hook", key="webhook_hook", name="W", relations={"target": [(db_job.id, "")]}
            )

    def test_relation_to_disallowed_kind_rejected(self, demo_store: Store):
        db_source = demo_store.components.create(_ORG, kind="source", key="demo_source", name="Demo")
        db_job = demo_store.components.create(_ORG, kind="job", key="cron_job", name="Job")
        # A job's 'target' may point at sources/assets — never at another job.
        with pytest.raises(ConfigError, match="may not point at a 'job'"):
            demo_store.components.create(
                _ORG, kind="job", key="cron_job", name="Bad", relations={"target": [(db_job.id, "")]}
            )
        # Sanity: the allowed kind passes.
        ok = demo_store.components.create(
            _ORG, kind="job", key="cron_job", name="Good", relations={"target": [(db_source.id, "")]}
        )
        assert ok.id is not None


class TestDependencySlotValidation:
    """Dependency writes are checked against the declared slots and their target identity."""

    @pytest.fixture
    def demo_store(self, component_db: Engine) -> Store:
        from interloper_assets.demo.source import DemoSource

        return Store(catalog=il.Catalog.from_assets([DemoSource]))

    @pytest.fixture
    def wire_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([WireUpSource, WireDownSource]))

    def test_undeclared_slot_rejected(self, demo_store: Store):
        source = demo_store.components.create(_ORG, kind="source", key="demo_source")
        with pytest.raises(ConfigError, match="declares no slot 'nope'"):
            demo_store.relations.add(
                _child(source, "b").id, type="dependency", dst_id=_child(source, "a").id, slot="nope"
            )

    def test_wrong_target_key_rejected(self, demo_store: Store):
        source = demo_store.components.create(_ORG, kind="source", key="demo_source")
        with pytest.raises(ConfigError, match="expects asset 'demo_source.a', got 'e'"):
            demo_store.relations.add(
                _child(source, "b").id, type="dependency", dst_id=_child(source, "e").id, slot="a"
            )

    def test_self_edge_rejected(self, store: Store):
        asset = store.components.create(_ORG, kind="asset", key="a")
        with pytest.raises(ConfigError, match="itself"):
            store.relations.add(asset.id, type="dependency", dst_id=asset.id, slot="x")

    def test_cross_instance_sibling_rejected(self, demo_store: Store):
        first = demo_store.components.create(_ORG, kind="source", key="demo_source")
        second = demo_store.components.create(_ORG, kind="source", key="demo_source", config={"dataset": "other"})
        with pytest.raises(ConfigError, match="sibling asset of the same source instance"):
            demo_store.relations.add(
                _child(first, "b").id, type="dependency", dst_id=_child(second, "a").id, slot="a"
            )

    def test_cross_source_dep_accepts_any_instance(self, wire_store: Store):
        up_two = wire_store.components.create(_ORG, kind="source", key="wire_up_source", config={"dataset": "two"})
        down = wire_store.components.create(_ORG, kind="source", key="wire_down_source")
        relation = wire_store.relations.add(
            _child(down, "consumer").id, type="dependency", dst_id=_child(up_two, "rows").id, slot="rows"
        )
        assert relation.dst_id == _child(up_two, "rows").id

    def test_cross_source_dep_rejects_wrong_source(self, component_db: Engine):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource, WireDownSource]))
        demo = store.components.create(_ORG, kind="source", key="demo_source")
        down = store.components.create(_ORG, kind="source", key="wire_down_source")
        with pytest.raises(ConfigError, match="expects asset 'wire_up_source.rows'"):
            store.relations.add(
                _child(down, "consumer").id, type="dependency", dst_id=_child(demo, "a").id, slot="rows"
            )


class TestRelationUpsert:
    """Slotted relation writes upsert per slot."""

    @pytest.fixture
    def wire_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([WireUpSource, WireDownSource]))

    def test_rebinding_a_slot_repoints_it(self, wire_store: Store):
        up_one = wire_store.components.create(_ORG, kind="source", key="wire_up_source")
        up_two = wire_store.components.create(_ORG, kind="source", key="wire_up_source", config={"dataset": "two"})
        down = wire_store.components.create(_ORG, kind="source", key="wire_down_source")
        consumer = _child(down, "consumer")

        wire_store.relations.add(consumer.id, type="dependency", dst_id=_child(up_one, "rows").id, slot="rows")
        wire_store.relations.add(consumer.id, type="dependency", dst_id=_child(up_two, "rows").id, slot="rows")

        (edge,) = wire_store.relations.list_all(_ORG, type="dependency")
        assert edge.dst_id == _child(up_two, "rows").id

    def test_identical_add_is_a_noop(self, wire_store: Store):
        up = wire_store.components.create(_ORG, kind="source", key="wire_up_source")
        down = wire_store.components.create(_ORG, kind="source", key="wire_down_source")
        consumer, rows = _child(down, "consumer"), _child(up, "rows")

        first = wire_store.relations.add(consumer.id, type="dependency", dst_id=rows.id, slot="rows")
        second = wire_store.relations.add(consumer.id, type="dependency", dst_id=rows.id, slot="rows")

        assert (second.src_id, second.dst_id, second.slot) == (first.src_id, first.dst_id, first.slot)
        assert len(wire_store.relations.list_all(_ORG, type="dependency")) == 1


class TestRequiredDependencyUnbindGuard:
    """Bound required dependency slots refuse unbinding (repoint instead)."""

    @pytest.fixture
    def dep_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([GuardUpstream, GuardRequired, GuardOptional]))

    def test_remove_required_dependency_refused(self, dep_store: Store):
        up = dep_store.components.create(_ORG, kind="asset", key="guard_upstream")
        down = dep_store.components.create(
            _ORG, kind="asset", key="guard_required", relations={"dependency": [(up.id, "up")]}
        )
        with pytest.raises(ConfigError, match="cannot be unbound"):
            dep_store.relations.remove(down.id, type="dependency", dst_id=up.id)
        assert len(dep_store.relations.list_all(_ORG, type="dependency")) == 1

    def test_remove_optional_dependency_allowed(self, dep_store: Store):
        up = dep_store.components.create(_ORG, kind="asset", key="guard_upstream")
        down = dep_store.components.create(
            _ORG, kind="asset", key="guard_optional", relations={"dependency": [(up.id, "up")]}
        )
        dep_store.relations.remove(down.id, type="dependency", dst_id=up.id)
        assert dep_store.relations.list_all(_ORG, type="dependency") == []

    def test_sync_clear_of_required_dependency_refused(self, dep_store: Store):
        up = dep_store.components.create(_ORG, kind="asset", key="guard_upstream")
        down = dep_store.components.create(
            _ORG, kind="asset", key="guard_required", relations={"dependency": [(up.id, "up")]}
        )
        with pytest.raises(ConfigError, match="cannot be unbound"):
            dep_store.components.update(down.id, relations={"dependency": []})

    def test_sync_repoint_of_required_dependency_allowed(self, dep_store: Store):
        up_one = dep_store.components.create(_ORG, kind="asset", key="guard_upstream")
        up_two = dep_store.components.create(_ORG, kind="asset", key="guard_upstream")
        down = dep_store.components.create(
            _ORG, kind="asset", key="guard_required", relations={"dependency": [(up_one.id, "up")]}
        )
        dep_store.components.update(down.id, relations={"dependency": [(up_two.id, "up")]})
        (edge,) = dep_store.relations.list_all(_ORG, type="dependency")
        assert edge.dst_id == up_two.id


class TestAddValidation:
    """``add`` vets the source, the vocabulary and the slot before writing."""

    def test_a_missing_source_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Component {missing} not found"):
            store.relations.add(missing, type="resource", dst_id=uuid4(), slot="connection")

    def test_a_slot_on_an_unslotted_relation_is_refused(self, component_db: Engine):
        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        source = store.components.create(_ORG, kind="source", key="demo_source")
        destination = store.components.create(_ORG, kind="destination", key="dest")

        with pytest.raises(ConfigError, match="is not slotted"):
            store.relations.add(source.id, type="destination", dst_id=destination.id, slot="nope")

    def test_a_slot_expecting_another_key_is_refused(self, component_db: Engine):
        # The slot declares the component key it accepts; anything else would
        # be wired into a resource the source cannot use.
        catalog = il.Catalog.from_assets([FacebookAds])
        store = Store(catalog=catalog, encrypt=lambda b: b, decrypt=lambda b: b)
        source = store.components.create(_ORG, kind="source", key="facebook_ads")
        wrong = store.components.create(_ORG, kind="destination", key="dest")

        with pytest.raises(ConfigError):
            store.relations.add(source.id, type="resource", dst_id=wrong.id, slot="connection")
