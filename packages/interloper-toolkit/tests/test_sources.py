"""Tests for ``interloper_toolkit.sources``."""

from __future__ import annotations

import dataclasses
from uuid import UUID, uuid4

import pytest
from interloper_db.models import Component
from interloper_db.store import Store

from interloper_toolkit import ToolkitContext, sources
from interloper_toolkit.models import ToolError


def _connection(store: Store, org_id: UUID, key: str = "demo_connection") -> Component:
    return store.components.create(org_id, kind="connection", key=key, config={}, encrypted=False)


class TestCreateSource:
    def test_creates_the_source_with_its_connection_assets_and_destinations(self, ctx: ToolkitContext, store: Store):
        connection = _connection(store, ctx.org_id)
        bq = store.components.create(ctx.org_id, kind="destination", key="bq")

        result = sources.create_source(
            ctx,
            "shop_source",
            "Shop",
            {},
            connection_id=str(connection.id),
            asset_keys=["orders"],
            destination_ids=[str(bq.id)],
        )

        assert result.status == "success"
        assert (result.asset_count, result.connection_bound, result.destination_count) == (1, True, 1)
        row = store.components.get(result.source.id)
        assert [c.key for c in row.children] == ["orders"]

    def test_a_required_connection_left_unbound_is_refused(self, ctx: ToolkitContext, store: Store):
        result = sources.create_source(ctx, "shop_source", "Shop", {})

        assert result.status == "error"
        assert "requires a 'demo_connection' as 'connection'" in result.error

    def test_unknown_asset_keys_list_the_valid_ones(self, ctx: ToolkitContext, store: Store):
        connection = _connection(store, ctx.org_id)

        result = sources.create_source(ctx, "shop_source", "Shop", {}, str(connection.id), asset_keys=["nope"])

        assert result.status == "error"
        assert result.valid_values == ["orders"]

    def test_another_orgs_connection_or_destination_is_not_found(self, ctx: ToolkitContext, store: Store):
        theirs = _connection(store, uuid4())
        mine = _connection(store, ctx.org_id)
        their_bq = store.components.create(uuid4(), kind="destination", key="bq")

        assert sources.create_source(ctx, "shop_source", "Shop", {}, str(theirs.id)).status == "error"
        by_dest = sources.create_source(
            ctx, "shop_source", "Shop", {}, str(mine.id), destination_ids=[str(their_bq.id)]
        )
        assert by_dest.status == "error"
        assert store.components.count(ctx.org_id, kinds=["source"]) == 0

    def test_a_viewer_is_refused(self, ctx: ToolkitContext, store: Store):
        connection = _connection(store, ctx.org_id)

        result = sources.create_source(
            dataclasses.replace(ctx, role="viewer"), "shop_source", "Shop", {}, str(connection.id)
        )

        assert isinstance(result, ToolError)
        assert store.components.count(ctx.org_id, kinds=["source"]) == 0


class TestCreateSources:
    def test_needs_a_single_account_field_or_an_explicit_one(self, ctx: ToolkitContext, store: Store):
        connection = _connection(store, ctx.org_id)

        implicit = sources.create_sources(ctx, "shop_source", [{"name": "A", "value": "1"}], str(connection.id))
        explicit = sources.create_sources(
            ctx, "shop_source", [{"name": "A", "value": "1"}], str(connection.id), field="nope"
        )

        assert implicit.status == "error"
        assert "no single account field" in implicit.error
        assert explicit.status == "error"
        assert "Unknown config field" in explicit.error


class TestSourceRelations:
    def test_binds_by_relation_key_and_reports_a_mismatch(self, ctx: ToolkitContext, store: Store):
        connection = _connection(store, ctx.org_id)
        defn = {"relations": {"connection": {"kind": "connection", "key": "other_connection", "optional": False}}}

        relations, error = sources.source_relations(ctx, defn, "shop_source", str(connection.id), None)

        assert relations is None
        assert error is not None
        assert "does not fit any relation of 'shop_source'" in error.error

    def test_an_optional_connection_may_stay_unbound(self, ctx: ToolkitContext):
        defn = {"relations": {"connection": {"kind": "connection", "key": "demo_connection", "optional": True}}}

        assert sources.source_relations(ctx, defn, "shop_source", None, None) == ({"destinations": []}, None)


class TestResolveSourceFieldOptions:
    async def test_a_field_that_is_not_provider_backed_is_refused(self, ctx: ToolkitContext, store: Store):
        connection = _connection(store, ctx.org_id)

        result = await sources.resolve_source_field_options(ctx, "shop_source", str(connection.id))

        assert result.status == "error"
        assert "fetchable" in result.error

    @pytest.mark.parametrize("source_key", ["nope", "demo_connection"])
    async def test_a_non_source_key_is_refused(self, ctx: ToolkitContext, source_key: str):
        result = await sources.resolve_source_field_options(ctx, source_key, str(uuid4()))

        assert result.status == "error"
