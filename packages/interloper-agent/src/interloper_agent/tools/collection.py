"""Collection tools — thin ADK wrappers over the shared toolkit.

The implementations (and the LLM-facing docstrings, adopted below) live in
``interloper_toolkit.collection`` and ``interloper_toolkit.sources``, so the
MCP server exposes the same logic. ``create_connections`` is the one write
the agent alone registers: its arguments carry credentials.
"""

from __future__ import annotations

from typing import Any

from google.adk.tools.tool_context import ToolContext
from interloper_toolkit import collection as toolkit_collection
from interloper_toolkit import sources as toolkit_sources

from interloper_agent.context import toolkit_ctx


def list_components(
    kind: str | None = None,
    q: str | None = None,
    limit: int = 50,
    offset: int = 0,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_collection.list_components(toolkit_ctx(tool_context), kind, q, limit, offset)
    return result.model_dump(mode="json")


def update_component(
    component_id: str,
    name: str | None = None,
    config_updates: dict[str, Any] | None = None,
    asset_keys: list[str] | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_collection.update_component(
        toolkit_ctx(tool_context), component_id, name, config_updates, asset_keys
    )
    return result.model_dump(mode="json")


def bind_relation(component_id: str, name: str, dst_id: str, tool_context: ToolContext | None = None) -> dict[str, Any]:
    return toolkit_collection.bind_relation(toolkit_ctx(tool_context), component_id, name, dst_id).model_dump(
        mode="json"
    )


def unbind_relation(
    component_id: str, name: str, dst_id: str, tool_context: ToolContext | None = None
) -> dict[str, Any]:
    result = toolkit_collection.unbind_relation(toolkit_ctx(tool_context), component_id, name, dst_id)
    return result.model_dump(mode="json")


def request_connection_setup(
    connection_key: str,
    name: str | None = None,
    force_new: bool = False,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_collection.request_connection_setup(toolkit_ctx(tool_context), connection_key, name, force_new)
    return result.model_dump(mode="json")


def create_connections(
    connection_key: str,
    instances: list[dict[str, Any]],
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_collection.create_connections(toolkit_ctx(tool_context), connection_key, instances)
    return result.model_dump(mode="json")


async def check_connection(connection_id: str, tool_context: ToolContext) -> dict[str, Any]:
    result = await toolkit_collection.check_connection(toolkit_ctx(tool_context), connection_id)
    return result.model_dump(mode="json")


async def resolve_source_field_options(
    source_key: str,
    connection_id: str,
    field: str | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = await toolkit_sources.resolve_source_field_options(
        toolkit_ctx(tool_context), source_key, connection_id, field
    )
    return result.model_dump(mode="json")


def create_source(
    source_key: str,
    name: str,
    config: dict[str, Any],
    connection_id: str | None = None,
    asset_keys: list[str] | None = None,
    destination_ids: list[str] | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_sources.create_source(
        toolkit_ctx(tool_context), source_key, name, config, connection_id, asset_keys, destination_ids
    )
    return result.model_dump(mode="json")


def create_sources(
    source_key: str,
    instances: list[dict[str, str]],
    connection_id: str | None = None,
    asset_keys: list[str] | None = None,
    shared_config: dict[str, Any] | None = None,
    field: str | None = None,
    destination_ids: list[str] | None = None,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    result = toolkit_sources.create_sources(
        toolkit_ctx(tool_context),
        source_key,
        instances,
        connection_id,
        asset_keys,
        shared_config,
        field,
        destination_ids,
    )
    return result.model_dump(mode="json")


def create_job(
    name: str,
    cron: str,
    target_source_ids: list[str],
    lookback: int | None = 1,
    offset: int = 1,
    tool_context: ToolContext | None = None,
) -> dict[str, Any]:
    from interloper_toolkit import jobs as toolkit_jobs

    result = toolkit_jobs.create_job(toolkit_ctx(tool_context), name, cron, target_source_ids, lookback, offset)
    return result.model_dump(mode="json")


list_components.__doc__ = toolkit_collection.list_components.__doc__
update_component.__doc__ = toolkit_collection.update_component.__doc__
bind_relation.__doc__ = toolkit_collection.bind_relation.__doc__
unbind_relation.__doc__ = toolkit_collection.unbind_relation.__doc__
request_connection_setup.__doc__ = toolkit_collection.request_connection_setup.__doc__
create_connections.__doc__ = toolkit_collection.create_connections.__doc__
check_connection.__doc__ = toolkit_collection.check_connection.__doc__
resolve_source_field_options.__doc__ = toolkit_sources.resolve_source_field_options.__doc__
create_source.__doc__ = toolkit_sources.create_source.__doc__
create_sources.__doc__ = toolkit_sources.create_sources.__doc__
