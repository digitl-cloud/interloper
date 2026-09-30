"""Tests for ``interloper_agent.interaction``."""

from __future__ import annotations

from interloper_toolkit import ToolkitContext
from interloper_toolkit.models import ToolError

from interloper_agent import interaction
from interloper_agent.interaction import SelectionRequest


class TestRequestUserSelection:
    def test_a_valid_request_awaits_the_user(self, ctx: ToolkitContext):
        result = interaction.request_user_selection(ctx, "Which account?", [{"label": "A", "value": "1"}])

        assert isinstance(result, SelectionRequest)
        assert result.awaits_user
        assert result.options == [{"label": "A", "value": "1"}]

    def test_an_empty_or_oversized_list_is_refused(self, ctx: ToolkitContext):
        assert isinstance(interaction.request_user_selection(ctx, "?", []), ToolError)
        too_many = [{"label": str(i), "value": str(i)} for i in range(51)]
        assert isinstance(interaction.request_user_selection(ctx, "?", too_many), ToolError)
