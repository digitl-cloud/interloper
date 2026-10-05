"""Tool behaviour over a real (in-memory) client-server session.

The properties under test: every tool carries the annotations a client keys
on, the writes are gated on the token's role, credentials never travel as
tool arguments, tool calls return the toolkit's structured results, and
everything is scoped to the authenticated organisation.
"""

from __future__ import annotations

import json
from typing import Any

import interloper as il
from interloper.settings import McpSettings
from interloper_db.store import Store
from interloper_toolkit import TOOLS
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


def _server(store: Store, catalog: il.Catalog, seeded: dict, role: str = "editor") -> Any:
    init_context(store, catalog)
    set_static_ctx(seeded["org"].id, role=role)
    return create_mcp_server(McpSettings(), store=None)._mcp_server


WRITES = {
    "update_component",
    "bind_relation",
    "unbind_relation",
    "create_source",
    "create_sources",
    "create_job",
    "toggle_job",
    "toggle_asset",
    "trigger_run",
    "retry_run",
    "cancel_run",
    "trigger_backfill",
    "cancel_backfill",
}


async def test_every_tool_is_annotated_and_credentials_never_travel_as_arguments(
    store: Store, catalog: il.Catalog, seeded: dict
):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    by_name = {t.name: t for t in tools}
    assert set(by_name) == {t.name for t in TOOLS if not t.carries_secrets}
    assert "create_connections" not in by_name
    assert WRITES <= set(by_name)
    assert all(t.annotations is not None for t in tools)
    reads = {name for name, t in by_name.items() if t.annotations and t.annotations.readOnlyHint}
    assert reads == set(by_name) - WRITES
    cancel, check = by_name["cancel_backfill"].annotations, by_name["check_connection"].annotations
    assert cancel is not None and cancel.destructiveHint is True
    assert check is not None and check.openWorldHint is True


async def test_a_viewer_token_is_refused_on_writes(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded, role="viewer")) as client:
        result = _result_dict(
            await client.call_tool("toggle_job", {"component_id": str(seeded["job_id"]), "enabled": False})
        )

    assert result == {
        "status": "error",
        "error": "Requires editor role or higher",
        "valid_values": None,
        "category": None,
    }
    assert (store.components.get(seeded["job_id"]).config or {})["enabled"] is True


async def test_an_editor_token_writes(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        result = _result_dict(
            await client.call_tool("toggle_job", {"component_id": str(seeded["job_id"]), "enabled": False})
        )

    assert result["status"] == "success"
    assert (store.components.get(seeded["job_id"]).config or {})["enabled"] is False


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
        result = _result_dict(await client.call_tool("run_stats", {"component_id": "not-a-uuid"}))

    assert result["status"] == "error"
    assert "error" in result


async def test_tools_declare_output_schemas(store: Store, catalog: il.Catalog, seeded: dict):
    async with create_connected_server_and_client_session(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    missing = [t.name for t in tools if not t.outputSchema]
    assert missing == []
    # Spot-check a computed shape made it into the schema.
    job_health = next(t for t in tools if t.name == "job_health")
    assert "JobHealthRow" in str(job_health.outputSchema)
