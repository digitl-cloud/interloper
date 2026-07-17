"""Analytics tools — thin ADK wrappers over the shared read-only toolkit.

The implementations (and the LLM-facing docstrings, adopted below) live in
``interloper_toolkit.analytics`` so the MCP server exposes the same logic.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import analytics as toolkit_analytics

from interloper_agent.context import toolkit_ctx


def run_history_summary(
    component_id: str | None = None,
    days: int = 7,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    return toolkit_analytics.run_history_summary(toolkit_ctx(tool_context), component_id, days)


def partition_coverage(
    component_id: str,
    start_date: str,
    end_date: str,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    return toolkit_analytics.partition_coverage(toolkit_ctx(tool_context), component_id, start_date, end_date)


def freshness_check(tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_analytics.freshness_check(toolkit_ctx(tool_context))


run_history_summary.__doc__ = toolkit_analytics.run_history_summary.__doc__
partition_coverage.__doc__ = toolkit_analytics.partition_coverage.__doc__
freshness_check.__doc__ = toolkit_analytics.freshness_check.__doc__
