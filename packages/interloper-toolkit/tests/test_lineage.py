"""Tests for ``interloper_toolkit.lineage``."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from interloper_db import engine as engine_module
from interloper_db.models import Component, ComponentRelation
from interloper_db.store import Store
from sqlmodel import Session

from interloper_toolkit import ToolkitContext, lineage


def _seed_chain(org_id: UUID) -> dict[str, Any]:
    """Seed source→assets a→b→c (b depends on a, c depends on b).

    Returns:
        The seeded component ids, keyed by asset key.

    """
    source = Component(org_id=org_id, kind="source", key="facebook_ads")
    a = Component(org_id=org_id, kind="asset", key="a", parent_id=source.id)
    b = Component(org_id=org_id, kind="asset", key="b", parent_id=source.id)
    c = Component(org_id=org_id, kind="asset", key="c", parent_id=source.id)
    deps = [
        ComponentRelation(src_id=b.id, dst_id=a.id, name="a", org_id=org_id, src_kind="asset", dst_kind="asset"),
        ComponentRelation(src_id=c.id, dst_id=b.id, name="b", org_id=org_id, src_kind="asset", dst_kind="asset"),
    ]
    ids = {"source": source.id, "a": a.id, "b": b.id, "c": c.id}
    with Session(engine_module.get_engine()) as session:
        session.add_all([source, a, b, c, *deps])
        session.commit()
    return ids


class TestLineage:
    def test_full_upstream_lineage_walks_the_chain(self, ctx: ToolkitContext):
        ids = _seed_chain(ctx.org_id)

        result = lineage.get_full_lineage(ctx, str(ids["c"]), direction="upstream")

        assert result.status == "success"
        assert result.lineage_count == 2
        assert [(item.asset_key, item.depth) for item in result.lineage] == [("b", 1), ("a", 2)]

    def test_impact_analysis_groups_downstream_by_source(self, ctx: ToolkitContext):
        ids = _seed_chain(ctx.org_id)

        result = lineage.impact_analysis(ctx, str(ids["a"]))

        assert result.status == "success"
        assert result.total_affected == 2
        assert {i.asset_key for i in result.by_source["facebook_ads"]} == {"b", "c"}

    def test_other_orgs_edges_are_invisible(self, ctx: ToolkitContext):
        ids = _seed_chain(org_id=uuid4())

        result = lineage.get_full_lineage(ctx, str(ids["c"]), direction="upstream")

        assert result.status == "success"
        assert result.lineage_count == 0

    def test_get_upstream_reports_relation_name(self, ctx: ToolkitContext, store: Store):
        shop = store.components.create(ctx.org_id, kind="source", key="shop_source")
        finance = store.components.create(ctx.org_id, kind="source", key="finance_source")
        orders = next(child for child in shop.children if child.key == "orders")
        revenue = next(child for child in finance.children if child.key == "revenue")
        store.relations.add(revenue.id, name="orders", dst_id=orders.id)

        result = lineage.get_upstream(ctx, str(revenue.id))

        assert result.status == "success"
        assert [(edge.param_name, edge.asset_id) for edge in result.upstream] == [("orders", str(orders.id))]

    def test_lineage_ignores_non_asset_relations(self, ctx: ToolkitContext, store: Store):
        shop = store.components.create(ctx.org_id, kind="source", key="shop_source")
        finance = store.components.create(ctx.org_id, kind="source", key="finance_source")
        orders = next(child for child in shop.children if child.key == "orders")
        revenue = next(child for child in finance.children if child.key == "revenue")
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")
        store.relations.add(revenue.id, name="orders", dst_id=orders.id)
        store.relations.add(finance.id, name="destinations", dst_id=bq.id)

        result = lineage.get_full_lineage(ctx, str(revenue.id), direction="upstream")

        assert result.status == "success"
        assert result.lineage_count == 1
        assert all(item.asset_key for item in result.lineage)
