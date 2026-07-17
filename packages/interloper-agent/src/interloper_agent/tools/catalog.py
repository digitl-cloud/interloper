"""Catalog tools — thin ADK wrappers over the shared read-only toolkit.

The implementations (and the LLM-facing docstrings, adopted below) live in
``interloper_toolkit.catalog`` so the MCP server exposes the same logic.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import catalog as toolkit_catalog

from interloper_agent.context import toolkit_ctx


def list_definitions(kind: str | None = None, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_catalog.list_definitions(toolkit_ctx(tool_context), kind)


def get_definition(key: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_catalog.get_definition(toolkit_ctx(tool_context), key)


def get_asset_schema(source_key: str, asset_key: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_catalog.get_asset_schema(toolkit_ctx(tool_context), source_key, asset_key)


def search_fields(query: str, tool_context: ToolContext) -> dict[str, Any]:
    return toolkit_catalog.search_fields(toolkit_ctx(tool_context), query)


def compare_schemas(
    source_key_a: str,
    asset_key_a: str,
    source_key_b: str,
    asset_key_b: str,
    tool_context: ToolContext,
) -> dict[str, Any]:
    return toolkit_catalog.compare_schemas(
        toolkit_ctx(tool_context), source_key_a, asset_key_a, source_key_b, asset_key_b
    )


list_definitions.__doc__ = toolkit_catalog.list_definitions.__doc__
get_definition.__doc__ = toolkit_catalog.get_definition.__doc__
get_asset_schema.__doc__ = toolkit_catalog.get_asset_schema.__doc__
search_fields.__doc__ = toolkit_catalog.search_fields.__doc__
compare_schemas.__doc__ = toolkit_catalog.compare_schemas.__doc__
