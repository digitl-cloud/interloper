"""Lineage tools — thin ADK wrappers over the shared read-only toolkit.

The implementations (and the LLM-facing docstrings, adopted below) live in
``interloper_db.toolkit.lineage`` so the MCP server exposes the same logic.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper_db.toolkit import lineage as toolkit_lineage

from interloper_agent.context import toolkit_ctx


def get_upstream(asset_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_lineage.get_upstream(toolkit_ctx(tool_context), asset_id)


def get_downstream(asset_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_lineage.get_downstream(toolkit_ctx(tool_context), asset_id)


def get_full_lineage(
    asset_id: str, direction: str = "upstream", tool_context: ToolContext | None = None
) -> dict[str, Any]:
    return toolkit_lineage.get_full_lineage(toolkit_ctx(tool_context), asset_id, direction)


def impact_analysis(asset_id: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_lineage.impact_analysis(toolkit_ctx(tool_context), asset_id)


def cross_source_dependencies(tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_lineage.cross_source_dependencies(toolkit_ctx(tool_context))


get_upstream.__doc__ = toolkit_lineage.get_upstream.__doc__
get_downstream.__doc__ = toolkit_lineage.get_downstream.__doc__
get_full_lineage.__doc__ = toolkit_lineage.get_full_lineage.__doc__
impact_analysis.__doc__ = toolkit_lineage.impact_analysis.__doc__
cross_source_dependencies.__doc__ = toolkit_lineage.cross_source_dependencies.__doc__
