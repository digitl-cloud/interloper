"""Tests for ``interloper_agent.interaction``."""

from __future__ import annotations

import pytest
from interloper_toolkit import ToolkitContext
from interloper_toolkit.models import ToolError
from pydantic_ai import CallDeferred

from interloper_agent import interaction


class TestRequestUserSelection:
    def test_a_valid_request_is_deferred_to_the_app(self, ctx: ToolkitContext):
        with pytest.raises(CallDeferred):
            interaction.request_user_selection(ctx, "Which account?", [{"label": "A", "value": "1"}])

    def test_an_empty_or_oversized_list_is_refused(self, ctx: ToolkitContext):
        assert isinstance(interaction.request_user_selection(ctx, "?", []), ToolError)
        too_many = [{"label": str(i), "value": str(i)} for i in range(51)]
        assert isinstance(interaction.request_user_selection(ctx, "?", too_many), ToolError)


class TestRequestConnectionSetup:
    def test_a_known_definition_is_deferred_to_the_app(self, ctx: ToolkitContext):
        with pytest.raises(CallDeferred):
            interaction.request_connection_setup(ctx, "demo_connection", name="Main")

    def test_an_unknown_key_is_refused_with_the_valid_ones(self, ctx: ToolkitContext):
        result = interaction.request_connection_setup(ctx, "nope")

        assert isinstance(result, ToolError)
        assert result.valid_values == ["demo_connection"]
