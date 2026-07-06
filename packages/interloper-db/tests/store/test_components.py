"""Tests for the shared component-relation machinery (``interloper_db.store.components``)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.store.components import list_components, load_component, sync_relations

_ORG = uuid4()


def _component(session: Session, kind: str, key: str, org_id: UUID = _ORG, **kwargs) -> Component:
    row = Component(org_id=org_id, kind=kind, key=key, **kwargs)
    session.add(row)
    session.flush()
    return row


def _relations(session: Session, src: Component, type: str | None = None) -> list[ComponentRelation]:
    statement = select(ComponentRelation).where(ComponentRelation.src_id == src.id)
    if type:
        statement = statement.where(ComponentRelation.type == type)
    return list(session.exec(statement).all())


class TestSyncRelations:
    """Replace semantics, denormalized stamping, and error handling."""

    def test_slot_map_stamps_org_and_kinds(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            connection = _component(session, "connection", "fake_connection")
            sync_relations(session, source, "resource", {"conn": connection.id})
            session.commit()

            (relation,) = _relations(session, source)
            assert (relation.type, relation.slot, relation.dst_id) == ("resource", "conn", connection.id)
            assert (relation.org_id, relation.src_kind, relation.dst_kind) == (_ORG, "source", "connection")

    def test_replaces_only_the_given_type(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            destination = _component(session, "destination", "dest")
            first = _component(session, "connection", "first")
            second = _component(session, "connection", "second")

            sync_relations(session, source, "destination", [destination.id])
            sync_relations(session, source, "resource", {"conn": first.id})
            sync_relations(session, source, "resource", {"conn": second.id})
            session.commit()

            resource_relations = _relations(session, source, "resource")
            assert [relation.dst_id for relation in resource_relations] == [second.id]
            assert len(_relations(session, source, "destination")) == 1

    def test_list_form_for_slotless_types(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            dest_a = _component(session, "destination", "a")
            dest_b = _component(session, "destination", "b")
            sync_relations(session, source, "destination", [dest_a.id, dest_b.id])
            session.commit()

            relations = _relations(session, source, "destination")
            assert {relation.dst_id for relation in relations} == {dest_a.id, dest_b.id}
            assert {relation.slot for relation in relations} == {""}

    def test_none_clears_the_type(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            destination = _component(session, "destination", "dest")
            sync_relations(session, source, "destination", [destination.id])
            sync_relations(session, source, "destination", None)
            session.commit()
            assert _relations(session, source) == []

    def test_rejects_missing_destination(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            with pytest.raises(NotFoundError):
                sync_relations(session, source, "destination", [uuid4()])

    def test_rejects_cross_org_destination(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            other = _component(session, "destination", "dest", org_id=uuid4())
            with pytest.raises(NotFoundError):
                sync_relations(session, source, "destination", [other.id])


class TestCascades:
    """FK cascade behaviour of the unified schema."""

    def test_deleting_a_component_cascades_its_relations(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            connection = _component(session, "connection", "conn")
            sync_relations(session, source, "resource", {"conn": connection.id})
            session.commit()

            session.delete(session.get(Component, connection.id))
            session.commit()
            assert _relations(session, source) == []

    def test_deleting_a_parent_cascades_children_and_their_relations(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            asset_a = _component(session, "asset", "a", parent_id=source.id)
            asset_b = _component(session, "asset", "b", parent_id=source.id)
            sync_relations(session, asset_b, "dependency", {"a": asset_a.id})
            session.commit()

            session.delete(session.get(Component, source.id))
            session.commit()

            assert session.exec(select(Component)).all() == []
            assert session.exec(select(ComponentRelation)).all() == []


class TestLoaders:
    """Kind-checked fetch and list helpers."""

    def test_load_component_checks_kind(self, component_db: Engine):
        with Session(component_db) as session:
            source = _component(session, "source", "demo_source")
            session.commit()
            assert load_component(session, source.id, kind="source").id == source.id
            with pytest.raises(NotFoundError):
                load_component(session, source.id, kind="asset")

    def test_list_components_filters_org_and_kind(self, component_db: Engine):
        with Session(component_db) as session:
            _component(session, "source", "mine")
            _component(session, "asset", "other_kind")
            _component(session, "source", "other_org", org_id=uuid4())
            session.commit()

            rows = list_components(session, _ORG, kind="source")
            assert [row.key for row in rows] == ["mine"]
