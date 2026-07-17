"""Tool behaviour over a real (in-memory) client-server session.

The properties under test: only the read-only tool surface is exposed, tool
calls return the toolkit's structured results, and everything is scoped to
the authenticated organisation.
"""

from __future__ import annotations

import json
from typing import Any

import interloper as il
from interloper.settings import McpSettings
from interloper_db.store import Store
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import CallToolResult

from interloper_mcp.context import init_context, set_static_ctx
from interloper_mcp.server import create_mcp_server


def _result_dict(result: CallToolResult) -> dict[str, Any]:
    payload = result.structuredContent
    if payload is None:
        payload = json.loads(result.content[0].text)  # ty: ignore[unresolved-attribute]
    # Union-typed returns are wrapped in a "result" envelope by FastMCP.
    return payload["result"] if set(payload) == {"result"} else payload


def _server(store: Store, catalog: il.Catalog, seeded: dict) -> Any:
    init_context(store, catalog)
    set_static_ctx(seeded["org"].id)
    return create_mcp_server(McpSettings(), store=None)._mcp_server


async def test_only_read_only_tools_are_exposed(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    names = {t.name for t in tools}
    assert len(names) == 20
    forbidden = {n for n in names if n.startswith(("trigger_", "toggle_", "create_", "request_"))}
    assert forbidden == set()
    assert {"list_jobs", "list_definitions", "get_full_lineage", "freshness_check"} <= names


async def test_list_jobs_returns_seeded_job_scoped_to_org(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        result = _result_dict(await client.call_tool("list_jobs", {}))

    assert result["status"] == "success"
    assert result["count"] == 1
    assert result["jobs"][0]["key"] == "daily_sync"  # the other org's job is invisible


async def test_list_recent_runs_returns_seeded_run(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        result = _result_dict(await client.call_tool("list_recent_runs", {"status": "success"}))

    assert result["status"] == "success"
    assert result["count"] == 1
    assert result["runs"][0]["component_id"] == str(seeded["job_id"])


async def test_tool_errors_are_structured_not_raised(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        result = _result_dict(await client.call_tool("get_job_health", {"component_id": "not-a-uuid"}))

    assert result["status"] == "error"
    assert "error" in result


async def test_tools_declare_output_schemas(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    missing = [t.name for t in tools if not t.outputSchema]
    assert missing == []
    # Spot-check a computed shape made it into the schema.
    job_health = next(t for t in tools if t.name == "get_job_health")
    assert "JobHealthStats" in str(job_health.outputSchema)
