"""Tests for ``interloper_agent.interaction``."""

from __future__ import annotations

import pytest
from interloper_db.store import Store
from interloper_toolkit import ToolkitContext
from interloper_toolkit.models import ToolError
from pydantic_ai import CallDeferred

from interloper_agent import interaction


class TestRequestUserSelection:
    def test_a_valid_request_defers_to_the_app(self, ctx: ToolkitContext):
        with pytest.raises(CallDeferred):
            interaction.request_user_selection(ctx, "Which account?", [{"label": "A", "value": "1"}])

    def test_an_empty_or_oversized_list_is_refused(self, ctx: ToolkitContext):
        assert isinstance(interaction.request_user_selection(ctx, "?", []), ToolError)
        too_many = [{"label": str(i), "value": str(i)} for i in range(51)]
        assert isinstance(interaction.request_user_selection(ctx, "?", too_many), ToolError)


class TestRequestConnectionSetup:
    def test_defers_to_the_form_when_nothing_fits(self, ctx: ToolkitContext):
        with pytest.raises(CallDeferred):
            interaction.request_connection_setup(ctx, "demo_connection", name="Main")

    def test_returns_the_existing_connections_or_the_error_instead(self, ctx: ToolkitContext, store: Store):
        store.components.create(ctx.org_id, kind="connection", key="demo_connection", config={})

        existing = interaction.request_connection_setup(ctx, "demo_connection")
        unknown = interaction.request_connection_setup(ctx, "nope")

        assert existing.status == "success"
        assert [c.key for c in existing.existing] == ["demo_connection"]
        assert isinstance(unknown, ToolError)
        with pytest.raises(CallDeferred):
            interaction.request_connection_setup(ctx, "demo_connection", force_new=True)
