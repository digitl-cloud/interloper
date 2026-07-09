"""Tests for the generic component store (``interloper_db.store.components``)."""

from __future__ import annotations

from uuid import uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, NotFoundError
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

    def test_delete_refuses_source_owned_assets(self, store: Store, component_db: Engine):
        job = store.create_component(_ORG, kind="job", key="cron_job")  # any parentable stand-in row
        with Session(component_db) as session:
            child = Component(org_id=_ORG, kind="asset", key="a", parent_id=job.id)
            session.add(child)
            session.commit()
            child_id = child.id
        with pytest.raises(ValueError):
            store.delete_component(child_id)

    def test_delete_cascades_relations(self, store: Store):
        job = store.create_component(_ORG, kind="job", key="cron_job", name="J")
        asset = store.create_component(_ORG, kind="asset", key="a")
        store.add_relation(job.id, type="target", dst_id=asset.id)

        store.delete_component(asset.id)
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


class TestRelationKindEnforcement:
    """Relation writes are checked against the vocabulary's allowed kinds."""

    @pytest.fixture
    def demo_store(self, component_db: Engine) -> Store:
        from interloper_assets.demo.source import DemoSource, demo_asset

        return Store(catalog=il.Catalog.from_assets([DemoSource, demo_asset]))

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
