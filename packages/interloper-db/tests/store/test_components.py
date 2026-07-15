"""Tests for the generic component store (``interloper_db.store.components``)."""

from __future__ import annotations

from typing import ClassVar
from uuid import uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, InUseError, NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.store import Store

_ORG = uuid4()


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store over the in-memory database (no catalog needed for these)."""
    return Store(catalog=il.Catalog(components={}))


def _relations(session: Session, src_id, type: str | None = None) -> list[ComponentRelation]:
    statement = select(ComponentRelation).where(ComponentRelation.src_id == src_id)
    if type:
        statement = statement.where(ComponentRelation.type == type)
    return list(session.exec(statement).all())


class TestRelations:
    """Replace semantics, vocabulary validation, denormalized stamping."""

    def test_sync_stamps_org_and_kinds(self, store: Store, component_db: Engine):
        dest = store.create_component(_ORG, kind="destination", key="dest")
        asset = store.create_component(_ORG, kind="asset", key="a", relations={"destination": [(dest.id, "")]})

        with Session(component_db) as session:
            (relation,) = _relations(session, asset.id)
            assert (relation.type, relation.slot, relation.dst_id) == ("destination", "", dest.id)
            assert (relation.org_id, relation.src_kind, relation.dst_kind) == (_ORG, "asset", "destination")

    def test_update_replaces_only_the_given_type(self, store: Store, component_db: Engine):
        dest = store.create_component(_ORG, kind="destination", key="dest")
        first = store.create_component(_ORG, kind="connection", key="first", config={}, encrypted=False)
        second = store.create_component(_ORG, kind="connection", key="second", config={}, encrypted=False)
        asset = store.create_component(
            _ORG,
            kind="asset",
            key="a",
            relations={"destination": [(dest.id, "")], "resource": [(first.id, "conn")]},
        )

        store.update_component(asset.id, relations={"resource": [(second.id, "conn")]})

        with Session(component_db) as session:
            assert [r.dst_id for r in _relations(session, asset.id, "resource")] == [second.id]
            assert len(_relations(session, asset.id, "destination")) == 1

    def test_empty_list_clears_the_type(self, store: Store, component_db: Engine):
        dest = store.create_component(_ORG, kind="destination", key="dest")
        asset = store.create_component(_ORG, kind="asset", key="a", relations={"destination": [(dest.id, "")]})
        store.update_component(asset.id, relations={"destination": []})
        with Session(component_db) as session:
            assert _relations(session, asset.id) == []

    def test_rejects_types_outside_the_kind_vocabulary(self, store: Store):
        dest = store.create_component(_ORG, kind="destination", key="dest")
        with pytest.raises(ConfigError):
            store.create_component(_ORG, kind="destination", key="d2", relations={"target": [(dest.id, "")]})

    def test_rejects_missing_and_cross_org_destinations(self, store: Store):
        other = store.create_component(uuid4(), kind="destination", key="dest")
        with pytest.raises(NotFoundError):
            store.create_component(_ORG, kind="asset", key="a", relations={"destination": [(uuid4(), "")]})
        with pytest.raises(NotFoundError):
            store.create_component(_ORG, kind="asset", key="b", relations={"destination": [(other.id, "")]})

    def test_add_and_remove_relation(self, store: Store):
        upstream = store.create_component(_ORG, kind="asset", key="a")
        downstream = store.create_component(_ORG, kind="asset", key="b")

        relation = store.add_relation(downstream.id, type="dependency", dst_id=upstream.id, slot="a")
        assert (relation.src_id, relation.dst_id, relation.slot) == (downstream.id, upstream.id, "a")
        assert len(store.list_relations(_ORG, type="dependency")) == 1

        store.remove_relation(downstream.id, type="dependency", dst_id=upstream.id)
        assert store.list_relations(_ORG) == []


class TestCrud:
    """Generic CRUD semantics shared by every kind."""

    def test_secret_kinds_encrypt_config_into_data(self, component_db: Engine):
        store = Store(catalog=il.Catalog(components={}), encrypt=lambda b: b[::-1], decrypt=lambda b: b[::-1])
        row = store.create_component(_ORG, kind="connection", key="conn", name="C", config={"token": "s3cret"})
        assert row.config is None
        assert row.encrypted is True
        assert store.decode_config(row) == {"token": "s3cret"}

    def test_secret_kinds_fail_closed_without_cipher(self, store: Store):
        with pytest.raises(ConfigError):
            store.create_component(_ORG, kind="connection", key="conn", config={"token": "s3cret"})

    def test_children_rejected_for_childless_kinds(self, store: Store):
        with pytest.raises(ConfigError):
            store.create_component(_ORG, kind="destination", key="dest", children=["a"])

    def test_unknown_child_keys_rejected(self, component_db: Engine):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        with pytest.raises(ConfigError, match=r"declares no asset\(s\) \['typo'\]"):
            store.create_component(_ORG, kind="source", key="demo_source", children=["a", "typo"])

    def test_delete_refuses_source_owned_assets(self, store: Store, component_db: Engine):
        job = store.create_component(_ORG, kind="job", key="cron_job")  # any parentable stand-in row
        with Session(component_db) as session:
            child = Component(org_id=_ORG, kind="asset", key="a", parent_id=job.id)
            session.add(child)
            session.commit()
            child_id = child.id
        with pytest.raises(ValueError):
            store.delete_component(child_id)

    def test_delete_source_removes_child_rows(self, component_db: Engine):
        from interloper_assets.demo.source import DemoSource

        store = Store(catalog=il.Catalog.from_assets([DemoSource]))
        source = store.create_component(_ORG, kind="source", key="demo_source")
        assert source.children

        store.delete_component(source.id)

        # The DB cascade must delete the children — a regression here leaves
        # them behind as orphaned parentless asset rows instead.
        with Session(component_db) as session:
            assert session.exec(select(Component).where(Component.kind == "asset")).all() == []

    def test_delete_cascades_outbound_relations(self, store: Store):
        job = store.create_component(_ORG, kind="job", key="cron_job", name="J")
        asset = store.create_component(_ORG, kind="asset", key="a")
        store.add_relation(job.id, type="target", dst_id=asset.id)

        store.delete_component(job.id)
        assert store.list_relations(_ORG) == []

    def test_list_filters_org_and_kinds(self, store: Store):
        store.create_component(_ORG, kind="destination", key="mine")
        store.create_component(_ORG, kind="asset", key="other_kind")
        store.create_component(uuid4(), kind="destination", key="other_org")

        rows = store.list_components(_ORG, kinds=["destination"])
        assert [row.key for row in rows] == ["mine"]
        assert {row.key for row in store.list_components(_ORG)} == {"mine", "other_kind"}

    def test_get_component_checks_kind(self, store: Store):
        dest = store.create_component(_ORG, kind="destination", key="dest")
        assert store.get_component(dest.id, kind="destination").id == dest.id
        with pytest.raises(NotFoundError):
            store.get_component(dest.id, kind="asset")


class TestDeleteInUseGuard:
    """A relation destination cannot be deleted while external referrers exist."""

    def _connection(self, store: Store) -> Component:
        return store.create_component(_ORG, kind="connection", key="conn", name="Conn", config={}, encrypted=False)

    def test_bound_connection_blocks_delete_and_names_referrer(self, store: Store):
        conn = self._connection(store)
        asset = store.create_component(_ORG, kind="asset", key="a", name="A", relations={"resource": [(conn.id, "c")]})

        with pytest.raises(InUseError) as excinfo:
            store.delete_component(conn.id)
        assert excinfo.value.referrers == [{"id": str(asset.id), "kind": "asset", "key": "a", "name": "A"}]
        assert "in use by A" in str(excinfo.value)

    def test_delete_succeeds_after_unbinding(self, store: Store):
        conn = self._connection(store)
        asset = store.create_component(_ORG, kind="asset", key="a", relations={"resource": [(conn.id, "c")]})

        store.remove_relation(asset.id, type="resource", dst_id=conn.id)
        store.delete_component(conn.id)
        with pytest.raises(NotFoundError):
            store.get_component(conn.id)

    def test_job_target_detaches(self, store: Store):
        asset = store.create_component(_ORG, kind="asset", key="a")
        job = store.create_component(_ORG, kind="job", key="cron_job", name="J", relations={"target": [(asset.id, "")]})

        store.delete_component(asset.id)
        assert store.get_component(job.id).id == job.id
        assert store.list_relations(_ORG) == []

    def test_hook_watch_detaches(self, store: Store):
        asset = store.create_component(_ORG, kind="asset", key="a")
        hook = store.create_component(_ORG, kind="hook", key="webhook", name="H", relations={"watch": [(asset.id, "")]})

        store.delete_component(asset.id)
        assert store.get_component(hook.id).id == hook.id
        assert store.list_relations(_ORG) == []

    def test_blocking_relation_wins_over_detaching(self, store: Store):
        conn = self._connection(store)
        asset = store.create_component(_ORG, kind="asset", key="a", name="A", relations={"resource": [(conn.id, "c")]})
        store.create_component(_ORG, kind="job", key="cron_job", relations={"target": [(asset.id, "")]})

        # The asset both consumes the connection (blocks its deletion) and is
        # a job target (detachable) — deleting the asset succeeds, deleting
        # the connection does not.
        with pytest.raises(InUseError):
            store.delete_component(conn.id)
        store.delete_component(asset.id)

    def test_referrer_through_child_reports_parent(self, store: Store, component_db: Engine):
        conn = self._connection(store)
        parent = store.create_component(_ORG, kind="job", key="cron_job", name="P")  # parentable stand-in
        with Session(component_db) as session:
            child = Component(org_id=_ORG, kind="asset", key="a", parent_id=parent.id)
            session.add(child)
            session.commit()
            child_id = child.id
        store.add_relation(child_id, type="resource", dst_id=conn.id, slot="c")

        with pytest.raises(InUseError) as excinfo:
            store.delete_component(conn.id)
        assert [r["id"] for r in excinfo.value.referrers] == [str(parent.id)]

    def test_intra_subtree_relations_do_not_block(self, store: Store, component_db: Engine):
        parent = store.create_component(_ORG, kind="job", key="cron_job", name="P")  # parentable stand-in
        with Session(component_db) as session:
            a = Component(org_id=_ORG, kind="asset", key="a", parent_id=parent.id)
            b = Component(org_id=_ORG, kind="asset", key="b", parent_id=parent.id)
            session.add(a)
            session.add(b)
            session.commit()
            a_id, b_id = a.id, b.id
        store.add_relation(b_id, type="dependency", dst_id=a_id, slot="a")

        store.delete_component(parent.id)
        with pytest.raises(NotFoundError):
            store.get_component(parent.id)


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
        up = dep_store.create_component(_ORG, kind="asset", key="guard_upstream", name="Up")
        down = dep_store.create_component(
            _ORG, kind="asset", key="guard_required", relations={"dependency": [(up.id, "up")]}
        )

        with pytest.raises(InUseError) as excinfo:
            dep_store.delete_component(up.id)
        assert [r["id"] for r in excinfo.value.referrers] == [str(down.id)]

    def test_optional_dependency_detaches(self, dep_store: Store):
        up = dep_store.create_component(_ORG, kind="asset", key="guard_upstream", name="Up")
        down = dep_store.create_component(
            _ORG, kind="asset", key="guard_optional", relations={"dependency": [(up.id, "up")]}
        )

        dep_store.delete_component(up.id)
        assert dep_store.get_component(down.id).id == down.id
        assert dep_store.list_relations(_ORG) == []


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
        guard_store.create_component(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.create_component(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})

    def test_distinct_alias_allowed(self, guard_store: Store):
        guard_store.create_component(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        second = guard_store.create_component(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "2"}
        )
        assert second.id is not None

    def test_alias_compared_after_sanitization(self, guard_store: Store):
        guard_store.create_component(_ORG, kind="source", key="discriminated_source", config={"account_id": "act-1"})
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.create_component(
                _ORG, kind="source", key="discriminated_source", config={"account_id": "ACT_1"}
            )

    def test_undiscriminated_source_needs_distinct_dataset(self, guard_store: Store):
        guard_store.create_component(_ORG, kind="source", key="demo_source")
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.create_component(_ORG, kind="source", key="demo_source")
        second = guard_store.create_component(_ORG, kind="source", key="demo_source", config={"dataset": "other"})
        assert second.id is not None

    def test_update_into_collision_rejected(self, guard_store: Store):
        guard_store.create_component(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        second = guard_store.create_component(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "2"}
        )
        with pytest.raises(ConfigError, match="materializing to"):
            guard_store.update_component(second.id, config={"account_id": "1"})

    def test_other_org_does_not_collide(self, guard_store: Store):
        guard_store.create_component(_ORG, kind="source", key="discriminated_source", config={"account_id": "1"})
        other = guard_store.create_component(
            uuid4(), kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        assert other.id is not None


class TestDerivedNames:
    """A blank ``components.name`` defaults to the instance's derived display name."""

    @pytest.fixture
    def name_store(self, component_db: Engine) -> Store:
        return Store(catalog=il.Catalog.from_assets([DiscriminatedSource]))

    def test_blank_name_defaults_to_instance_name(self, name_store: Store):
        row = name_store.create_component(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        assert row.name == "1"

    def test_explicit_name_wins(self, name_store: Store):
        row = name_store.create_component(
            _ORG, kind="source", key="discriminated_source", name="Mine", config={"account_id": "1"}
        )
        assert row.name == "Mine"

    def test_default_name_follows_config_change(self, name_store: Store):
        row = name_store.create_component(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        updated = name_store.update_component(row.id, config={"account_id": "2"})
        assert updated.name == "2"

    def test_customized_name_untouched_by_config_change(self, name_store: Store):
        row = name_store.create_component(
            _ORG, kind="source", key="discriminated_source", config={"account_id": "1"}
        )
        name_store.update_component(row.id, name="Mine")
        updated = name_store.update_component(row.id, config={"account_id": "2"})
        assert updated.name == "Mine"

    def test_unresolvable_key_leaves_name_blank(self, store: Store):
        row = store.create_component(_ORG, kind="destination", key="ghost")
        assert row.name is None


class TestRelationKindEnforcement:
    """Relation writes are checked against the vocabulary's allowed kinds."""

    @pytest.fixture
    def demo_store(self, component_db: Engine) -> Store:
        from interloper_assets.demo.source import DemoSource, demo_asset

        return Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset]))

    def test_class_vocabulary_governs_writes(self, demo_store: Store):
        db_job = demo_store.create_component(_ORG, kind="job", key="cron_job", name="J")
        # TriggerHook declares `target`; WebhookHook does not.
        ok = demo_store.create_component(
            _ORG, kind="hook", key="trigger_hook", name="T", relations={"target": [(db_job.id, "")]}
        )
        assert ok.id is not None
        with pytest.raises(ConfigError, match="'webhook_hook'.*declare no 'target' relations"):
            demo_store.create_component(
                _ORG, kind="hook", key="webhook_hook", name="W", relations={"target": [(db_job.id, "")]}
            )

    def test_relation_to_disallowed_kind_rejected(self, demo_store: Store):
        db_source = demo_store.create_component(_ORG, kind="source", key="demo_source", name="Demo")
        db_job = demo_store.create_component(_ORG, kind="job", key="cron_job", name="Job")
        # A job's 'target' may point at sources/assets — never at another job.
        with pytest.raises(ConfigError, match="may not point at a 'job'"):
            demo_store.create_component(
                _ORG, kind="job", key="cron_job", name="Bad", relations={"target": [(db_job.id, "")]}
            )
        # Sanity: the allowed kind passes.
        ok = demo_store.create_component(
            _ORG, kind="job", key="cron_job", name="Good", relations={"target": [(db_source.id, "")]}
        )
        assert ok.id is not None
