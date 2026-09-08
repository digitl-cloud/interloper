"""Tests for the relation read/write layer (``interloper_db.store.relations``)."""

from __future__ import annotations

from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation
from interloper_db.store import Store

_ORG = uuid4()


def _relations(session: Session, src_id: UUID, name: str | None = None) -> list[ComponentRelation]:
    statement = select(ComponentRelation).where(ComponentRelation.src_id == src_id)
    if name:
        statement = statement.where(ComponentRelation.name == name)
    return list(session.exec(statement).all())


def _child(source: Component, key: str) -> Component:
    return next(child for child in source.children if child.key == key)


class WireConnection(il.Connection):
    """Connection the wire sources bind."""


class GuardUpstream(il.Asset):
    """Upstream asset for the unbind-guard tests."""

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class GuardOther(il.Asset):
    """Asset no guard relation declares a key for."""

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class GuardRequired(il.Asset):
    """Asset with a required upstream on ``guard_upstream``."""

    up = il.Relation("asset", "guard_upstream")

    def data(self, context: il.ExecutionContext, up: il.Upstream) -> list[dict]:
        return []


class GuardOptional(il.Asset):
    """Asset with an optional upstream on ``guard_upstream``."""

    up = il.Relation("asset", "guard_upstream", optional=True)

    def data(self, context: il.ExecutionContext, up: il.Upstream | None) -> list[dict]:
        return []


class Matcher(il.Asset):
    """Asset fanning in the ``campaigns`` asset of every source."""

    campaigns: list[il.Asset] = il.Relation("asset", "*.campaigns", many=True)

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class WireUpSource(il.Source):
    """Upstream source whose ``totals`` reads its sibling ``rows``."""

    class Rows(il.Asset):
        """Root asset of the source."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []

    class Totals(il.Asset):
        """Asset reading the source's own ``rows``."""

        rows = il.Relation("asset", "rows")

        def data(self, context: il.ExecutionContext, rows: il.Upstream) -> list[dict]:
            return []


class WireOtherSource(il.Source):
    """Another source declaring a ``rows`` asset, of a different source key."""

    class Rows(il.Asset):
        """Homonym of ``WireUpSource.Rows``, owned by another source."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


class WireDownSource(il.Source):
    """Downstream source: a bound connection and a cross-source upstream."""

    connection: WireConnection

    class Consumer(il.Asset):
        """Asset reading ``wire_up_source.rows``."""

        rows = il.Relation("asset", "wire_up_source.rows")

        def data(self, context: il.ExecutionContext, rows: il.Upstream) -> list[dict]:
            return []


class FirstCampaignSource(il.Source):
    """Source declaring a ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaign entities of the first provider."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


class SecondCampaignSource(il.Source):
    """Another source declaring a ``campaigns`` asset."""

    class Campaigns(il.Asset):
        """Campaign entities of the second provider."""

        def data(self, context: il.ExecutionContext) -> list[dict]:
            return []


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store whose catalog carries every class the relation tests declare.

    Returns:
        A store reading and writing the fixture database.
    """
    catalog = il.Catalog.from_assets(
        [
            GuardUpstream,
            GuardOther,
            GuardRequired,
            GuardOptional,
            Matcher,
            WireUpSource,
            WireOtherSource,
            WireDownSource,
            FirstCampaignSource,
            SecondCampaignSource,
        ]
    )
    return Store(catalog=catalog)


@pytest.fixture
def connection(store: Store) -> Component:
    """A plaintext connection row the wire sources accept.

    Returns:
        The created connection component.
    """
    return store.components.create(_ORG, kind="connection", key="wire_connection", config={}, encrypted=False)


class TestAdd:
    """``add`` binds by name, through the declared relation's acceptance rule."""

    def test_single_valued_relation_repoints(self, store: Store, connection: Component, component_db: Engine):
        other = store.components.create(_ORG, kind="connection", key="wire_connection", config={}, encrypted=False)
        source = store.components.create(_ORG, kind="source", key="wire_down_source")

        store.relations.add(source.id, name="connection", dst_id=connection.id)
        store.relations.add(source.id, name="connection", dst_id=other.id)

        with Session(component_db) as session:
            assert [r.dst_id for r in _relations(session, source.id, "connection")] == [other.id]

    def test_many_valued_relation_accumulates(self, store: Store, component_db: Engine):
        matcher = store.components.create(_ORG, kind="asset", key="matcher")
        first = store.components.create(_ORG, kind="source", key="first_campaign_source")
        second = store.components.create(_ORG, kind="source", key="second_campaign_source")

        store.relations.add(matcher.id, name="campaigns", dst_id=_child(first, "campaigns").id)
        store.relations.add(matcher.id, name="campaigns", dst_id=_child(second, "campaigns").id)

        with Session(component_db) as session:
            assert len(_relations(session, matcher.id, "campaigns")) == 2

    def test_wildcard_key_refuses_a_parentless_asset(self, store: Store):
        matcher = store.components.create(_ORG, kind="asset", key="matcher")
        standalone = store.components.create(_ORG, kind="asset", key="campaigns")

        with pytest.raises(ConfigError, match="does not accept"):
            store.relations.add(matcher.id, name="campaigns", dst_id=standalone.id)

    def test_rejects_a_kind_the_relation_does_not_declare(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")
        destination = store.components.create(_ORG, kind="destination", key="dest")

        with pytest.raises(ConfigError, match="does not accept"):
            store.relations.add(source.id, name="connection", dst_id=destination.id)

    def test_rejects_an_undeclared_name(self, store: Store, connection: Component):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")

        with pytest.raises(ConfigError, match="declares no relation 'nope'"):
            store.relations.add(source.id, name="nope", dst_id=connection.id)

    def test_rejects_an_undeclared_key(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="guard_other")
        required = store.components.create(_ORG, kind="asset", key="guard_required")

        with pytest.raises(ConfigError, match="does not accept"):
            store.relations.add(required.id, name="up", dst_id=upstream.id)

    def test_checks_a_declared_key_against_the_parent_source(self, store: Store):
        wire_up = store.components.create(_ORG, kind="source", key="wire_up_source")
        other = store.components.create(_ORG, kind="source", key="wire_other_source")
        down = store.components.create(_ORG, kind="source", key="wire_down_source", config={})
        consumer = _child(down, "consumer")

        with pytest.raises(ConfigError, match="does not accept"):
            store.relations.add(consumer.id, name="rows", dst_id=_child(other, "rows").id)

        relation = store.relations.add(consumer.id, name="rows", dst_id=_child(wire_up, "rows").id)
        assert relation.dst_id == _child(wire_up, "rows").id

    def test_identical_add_returns_the_existing_row(self, store: Store, connection: Component):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")

        first = store.relations.add(source.id, name="connection", dst_id=connection.id)
        second = store.relations.add(source.id, name="connection", dst_id=connection.id)

        assert (second.src_id, second.name, second.dst_id) == (first.src_id, first.name, first.dst_id)
        assert len(store.relations.list_all(_ORG, name="connection")) == 1

    def test_stamps_the_denormalized_org_and_kinds(self, store: Store, connection: Component):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")

        relation = store.relations.add(source.id, name="connection", dst_id=connection.id)

        assert (relation.org_id, relation.src_kind, relation.dst_kind) == (_ORG, "source", "connection")

    def test_a_missing_source_raises(self, store: Store, connection: Component):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Component {missing} not found"):
            store.relations.add(missing, name="connection", dst_id=connection.id)

    def test_a_cross_org_target_raises(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")
        foreign = store.components.create(uuid4(), kind="connection", key="wire_connection", config={}, encrypted=False)

        with pytest.raises(NotFoundError, match=f"Component {foreign.id} not found"):
            store.relations.add(source.id, name="connection", dst_id=foreign.id)


class TestRemove:
    """``remove`` refuses to empty a non-optional relation."""

    def test_last_row_of_a_required_relation_is_refused(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="guard_upstream")
        required = store.components.create(_ORG, kind="asset", key="guard_required", relations={"up": [upstream.id]})

        with pytest.raises(ConfigError, match="non-optional"):
            store.relations.remove(required.id, name="up", dst_id=upstream.id)
        assert len(store.relations.list_all(_ORG, name="up")) == 1

    def test_last_row_of_an_optional_relation_detaches(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="guard_upstream")
        optional = store.components.create(_ORG, kind="asset", key="guard_optional", relations={"up": [upstream.id]})

        store.relations.remove(optional.id, name="up", dst_id=upstream.id)

        assert store.relations.list_all(_ORG, name="up") == []

    def test_an_absent_row_is_a_no_op(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="guard_upstream")
        required = store.components.create(_ORG, kind="asset", key="guard_required")

        store.relations.remove(required.id, name="up", dst_id=upstream.id)

        assert store.relations.list_all(_ORG) == []


class TestListAll:
    """``list_all`` filters by name and by either endpoint's kind."""

    def test_filters_by_kinds(self, store: Store, connection: Component):
        upstream = store.components.create(_ORG, kind="asset", key="guard_upstream")
        store.components.create(_ORG, kind="asset", key="guard_required", relations={"up": [upstream.id]})
        source = store.components.create(_ORG, kind="source", key="wire_down_source")
        store.relations.add(source.id, name="connection", dst_id=connection.id)

        rows = store.relations.list_all(_ORG, src_kind="asset", dst_kind="asset")

        assert {row.name for row in rows} == {"up"}

    def test_filters_by_name(self, store: Store, connection: Component):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")
        store.relations.add(source.id, name="connection", dst_id=connection.id)

        assert len(store.relations.list_all(_ORG, name="connection")) == 1
        assert store.relations.list_all(_ORG, name="up") == []

    def test_is_scoped_to_the_organisation(self, store: Store, connection: Component):
        source = store.components.create(_ORG, kind="source", key="wire_down_source")
        store.relations.add(source.id, name="connection", dst_id=connection.id)

        assert store.relations.list_all(uuid4()) == []


class TestSyncRelations:
    """``_sync_relations`` replaces each listed name wholesale."""

    def test_replaces_a_many_valued_relation_wholesale(self, store: Store, component_db: Engine):
        first = store.components.create(_ORG, kind="source", key="first_campaign_source")
        second = store.components.create(_ORG, kind="source", key="second_campaign_source")
        matcher = store.components.create(
            _ORG,
            kind="asset",
            key="matcher",
            relations={"campaigns": [_child(first, "campaigns").id, _child(second, "campaigns").id]},
        )

        store.components.update(matcher.id, relations={"campaigns": [_child(second, "campaigns").id]})

        with Session(component_db) as session:
            assert [r.dst_id for r in _relations(session, matcher.id, "campaigns")] == [_child(second, "campaigns").id]

    def test_leaves_the_names_it_is_not_given_alone(self, store: Store, connection: Component, component_db: Engine):
        destination = store.components.create(_ORG, kind="destination", key="dest")
        source = store.components.create(
            _ORG,
            kind="source",
            key="wire_down_source",
            relations={"connection": [connection.id], "destinations": [destination.id]},
        )

        store.components.update(source.id, relations={"destinations": []})

        with Session(component_db) as session:
            assert len(_relations(session, source.id, "connection")) == 1
            assert _relations(session, source.id, "destinations") == []

    def test_refuses_to_empty_a_non_optional_relation(self, store: Store):
        upstream = store.components.create(_ORG, kind="asset", key="guard_upstream")
        required = store.components.create(_ORG, kind="asset", key="guard_required", relations={"up": [upstream.id]})

        with pytest.raises(ConfigError, match="non-optional"):
            store.components.update(required.id, relations={"up": []})

    def test_repointing_a_non_optional_relation_is_allowed(self, store: Store):
        first = store.components.create(_ORG, kind="asset", key="guard_upstream")
        second = store.components.create(_ORG, kind="asset", key="guard_upstream")
        required = store.components.create(_ORG, kind="asset", key="guard_required", relations={"up": [first.id]})

        store.components.update(required.id, relations={"up": [second.id]})

        (row,) = store.relations.list_all(_ORG, name="up")
        assert row.dst_id == second.id

    def test_rejects_an_undeclared_name(self, store: Store, connection: Component):
        with pytest.raises(ConfigError, match="declares no relation 'nope'"):
            store.components.create(_ORG, kind="source", key="wire_down_source", relations={"nope": [connection.id]})

    def test_rejects_several_targets_on_a_single_valued_relation(self, store: Store, connection: Component):
        other = store.components.create(_ORG, kind="connection", key="wire_connection", config={}, encrypted=False)

        with pytest.raises(ConfigError, match="single-valued"):
            store.components.create(
                _ORG,
                kind="source",
                key="wire_down_source",
                relations={"connection": [connection.id, other.id]},
            )


class TestIntraSourceWiring:
    """A source's own sibling relations are wired from the class declaration."""

    def test_creating_a_source_wires_its_sibling_relations(self, store: Store):
        source = store.components.create(_ORG, kind="source", key="wire_up_source")

        (row,) = store.relations.list_all(_ORG, name="rows")
        assert (row.src_id, row.dst_id) == (_child(source, "totals").id, _child(source, "rows").id)
