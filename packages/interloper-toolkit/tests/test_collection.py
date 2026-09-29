"""Tests for ``interloper_toolkit.collection``."""

from __future__ import annotations

from uuid import uuid4

from interloper_db.store import Store

from interloper_toolkit import ToolkitContext, collection


class TestBindRelation:
    def test_bind_relation_creates_row(self, ctx: ToolkitContext, store: Store):
        source = store.components.create(ctx.org_id, kind="source", key="shop_source")
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")

        result = collection.bind_relation(ctx, str(source.id), "destinations", str(bq.id))

        assert result.status == "success"
        assert (result.name, result.dst_kind) == ("destinations", "destination")

    def test_bind_relation_wrong_kind_is_tool_error(self, ctx: ToolkitContext, store: Store):
        source = store.components.create(ctx.org_id, kind="source", key="shop_source")
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")

        result = collection.bind_relation(ctx, str(source.id), "connection", str(bq.id))

        assert result.status == "error"
        assert "does not accept" in result.error

    def test_bind_relation_bad_uuid_is_tool_error(self, ctx: ToolkitContext):
        result = collection.bind_relation(ctx, "not-a-uuid", "destinations", "also-not-a-uuid")

        assert result.status == "error"

    def test_unbind_relation_removes_row(self, ctx: ToolkitContext, store: Store):
        source = store.components.create(ctx.org_id, kind="source", key="shop_source")
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")
        store.relations.add(source.id, name="destinations", dst_id=bq.id)

        result = collection.unbind_relation(ctx, str(source.id), "destinations", str(bq.id))

        assert result.status == "success"
        assert store.relations.list_all(ctx.org_id, name="destinations") == []

    def test_bind_relation_from_another_orgs_component_is_not_found(self, ctx: ToolkitContext, store: Store):
        other_org = uuid4()
        source = store.components.create(other_org, kind="source", key="shop_source")
        bq = store.components.create(other_org, kind="destination", key="bq")

        result = collection.bind_relation(ctx, str(source.id), "destinations", str(bq.id))

        assert result.status == "error"
        assert store.relations.list_all(other_org, name="destinations") == []

    def test_unbind_relation_from_another_orgs_component_is_not_found(self, ctx: ToolkitContext, store: Store):
        other_org = uuid4()
        source = store.components.create(other_org, kind="source", key="shop_source")
        bq = store.components.create(other_org, kind="destination", key="bq")
        store.relations.add(source.id, name="destinations", dst_id=bq.id)

        result = collection.unbind_relation(ctx, str(source.id), "destinations", str(bq.id))

        assert result.status == "error"
        assert len(store.relations.list_all(other_org, name="destinations")) == 1
