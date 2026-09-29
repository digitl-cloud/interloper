"""Tests for ``interloper_toolkit.collection``."""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from uuid import uuid4

import pytest
from interloper.settings import AppSettings, ServerSettings
from interloper_db.store import Store

from interloper_toolkit import ToolkitContext, collection
from interloper_toolkit.models import ComponentCounts, ComponentList, ToolError


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


class TestListComponents:
    def test_searches_and_pages_with_a_total(self, ctx: ToolkitContext, store: Store):
        store.components.create(ctx.org_id, kind="destination", key="bq", name="Raw warehouse")
        store.components.create(ctx.org_id, kind="destination", key="bq", name="Clean warehouse")
        store.components.create(ctx.org_id, kind="destination", key="bq", name="Lake")

        page = collection.list_components(ctx, kind="destination", q="warehouse", limit=1, offset=1)
        counts = collection.list_components(ctx, q="lake")

        assert isinstance(page, ComponentList)
        assert (page.count, page.total) == (1, 2)
        assert page.components[0].name == "Clean warehouse"
        assert isinstance(counts, ComponentCounts)
        assert counts.component_counts == {"destination": 1}


class TestUpdateComponent:
    def _source(self, ctx: ToolkitContext, store: Store) -> str:
        connection = store.components.create(
            ctx.org_id, kind="connection", key="demo_connection", config={}, encrypted=False
        )
        row = store.components.create(
            ctx.org_id, kind="source", key="shop_source", relations={"connection": [connection.id], "destinations": []}
        )
        return str(row.id)

    def test_renames_and_merges_partial_config(self, ctx: ToolkitContext, store: Store):
        bq = store.components.create(
            ctx.org_id, kind="destination", key="bq", config={"dataset": "raw", "location": "EU"}
        )

        result = collection.update_component(
            ctx, str(bq.id), name="Warehouse", config_updates={"dataset": "clean", "location": None}
        )

        assert result.status == "success"
        assert result.changed_fields == ["dataset", "location"]
        assert result.component.name == "Warehouse"
        assert store.components.get(bq.id).config == {"dataset": "clean"}

    def test_replaces_a_sources_asset_selection(self, ctx: ToolkitContext, store: Store):
        source_id = self._source(ctx, store)

        result = collection.update_component(ctx, source_id, asset_keys=["orders"])

        assert result.status == "success"
        assert result.asset_count == 1
        assert collection.update_component(ctx, source_id, asset_keys=["nope"]).status == "error"

    def test_refuses_assets_on_other_kinds_and_credentials_on_connections(self, ctx: ToolkitContext, store: Store):
        connection = store.components.create(
            ctx.org_id, kind="connection", key="demo_connection", config={}, encrypted=False
        )
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")

        assets = collection.update_component(ctx, str(bq.id), asset_keys=["orders"])
        secrets = collection.update_component(ctx, str(connection.id), config_updates={"token": "x"})
        rename = collection.update_component(ctx, str(connection.id), name="Main")

        assert assets.status == "error"
        assert secrets.status == "error"
        assert "credentials" in secrets.error
        assert rename.status == "success"

    def test_requires_a_change_and_the_orgs_own_component(self, ctx: ToolkitContext, store: Store):
        mine = store.components.create(ctx.org_id, kind="destination", key="bq")
        theirs = store.components.create(uuid4(), kind="destination", key="bq")

        assert collection.update_component(ctx, str(mine.id)).status == "error"
        assert collection.update_component(ctx, str(theirs.id), name="Hijack").status == "error"

    def test_a_viewer_is_refused(self, ctx: ToolkitContext, store: Store):
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")

        result = collection.update_component(dataclasses.replace(ctx, role="viewer"), str(bq.id), name="Nope")

        assert isinstance(result, ToolError)
        assert store.components.get(bq.id).name != "Nope"


@pytest.fixture
def external_url(request: pytest.FixtureRequest) -> Iterator[str]:
    """Activate settings whose public app URL is the parametrised value.

    Yields:
        The URL the settings carry.
    """
    settings = AppSettings.model_construct(server=ServerSettings(external_url=request.param))
    AppSettings.activate(settings)
    try:
        yield request.param
    finally:
        AppSettings.clear_active()


class TestConnections:
    @pytest.mark.parametrize("external_url", ["https://app.example.com/", ""], indirect=True)
    def test_request_connection_setup_links_the_form_when_the_app_has_a_public_url(
        self, ctx: ToolkitContext, external_url: str
    ):
        result = collection.request_connection_setup(ctx, "demo_connection", name="Main account")

        assert result.status == "success"
        expected = "https://app.example.com/components/connections?new=demo_connection&name=Main+account"
        assert result.setup_url == (expected if external_url else None)

    def test_request_connection_setup_presents_a_form_or_the_existing_connections(
        self, ctx: ToolkitContext, store: Store
    ):
        fresh = collection.request_connection_setup(ctx, "demo_connection", name="Main")
        store.components.create(ctx.org_id, kind="connection", key="demo_connection", config={}, encrypted=False)
        again = collection.request_connection_setup(ctx, "demo_connection")
        forced = collection.request_connection_setup(ctx, "demo_connection", force_new=True)
        unknown = collection.request_connection_setup(ctx, "nope")

        assert fresh.status == "success"
        assert again.status == "success"
        assert forced.status == "success"
        assert (fresh.name, fresh.oauth, fresh.existing) == ("Main", False, [])
        assert [c.key for c in again.existing] == ["demo_connection"]
        assert forced.existing == []
        assert unknown.status == "error"
        assert unknown.valid_values == ["demo_connection"]

    def test_create_connections_reports_each_instance(self, ctx: ToolkitContext, store: Store):
        result = collection.create_connections(
            ctx, "demo_connection", [{"name": "A", "config": {}}, {"config": {}}, {"name": "B", "config": {}}]
        )

        assert result.status == "success"
        assert [c.name for c in result.created] == ["A", "B"]
        assert result.failed == []
        assert store.components.count(ctx.org_id, kinds=["connection"]) == 2
        assert collection.create_connections(ctx, "demo_connection", []).status == "error"

    def test_create_connections_is_gated(self, ctx: ToolkitContext, store: Store):
        viewer = dataclasses.replace(ctx, role="viewer")

        assert isinstance(
            collection.create_connections(viewer, "demo_connection", [{"name": "A", "config": {}}]), ToolError
        )
        assert store.components.count(ctx.org_id, kinds=["connection"]) == 0

    async def test_check_connection_reports_a_type_without_a_live_check(self, ctx: ToolkitContext, store: Store):
        connection = store.components.create(
            ctx.org_id, kind="connection", key="demo_connection", config={}, encrypted=False
        )

        result = await collection.check_connection(ctx, str(connection.id))

        assert result.status == "success"
        assert (result.ok, result.live) == (True, False)
        assert (await collection.check_connection(ctx, str(uuid4()))).status == "error"


class TestRelationGate:
    def test_bind_and_unbind_refuse_a_viewer(self, ctx: ToolkitContext, store: Store):
        source = store.components.create(ctx.org_id, kind="source", key="shop_source")
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")
        viewer = dataclasses.replace(ctx, role="viewer")

        assert isinstance(collection.bind_relation(viewer, str(source.id), "destinations", str(bq.id)), ToolError)
        assert isinstance(collection.unbind_relation(viewer, str(source.id), "destinations", str(bq.id)), ToolError)
