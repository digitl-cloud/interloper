"""Tool behaviour over a real (in-memory) client-server session.

The properties under test: every tool carries the annotations a client keys
on and takes the organisation it acts in, the writes are gated on the
caller's role there, credentials never travel as tool arguments, tool calls
return the toolkit's structured results, and everything is scoped to the
named organisation.
"""

from __future__ import annotations

import json
from typing import Any

import interloper as il
from interloper.settings import McpSettings
from interloper_db.store import Store
from interloper_toolkit import TOOLS
from mcp import Client
from mcp.server.mcpserver import MCPServer
from mcp.types import CallToolResult

from interloper_mcp.context import init_context, set_static_user
from interloper_mcp.server import create_mcp_server


def _result_dict(result: CallToolResult) -> dict[str, Any]:
    payload = result.structured_content
    if payload is None:
        payload = json.loads(result.content[0].text)  # ty: ignore[unresolved-attribute]
    # Union-typed returns are wrapped in a "result" envelope by MCPServer.
    return payload["result"] if set(payload) == {"result"} else payload


def _server(store: Store, catalog: il.Catalog, seeded: dict, role: str = "editor") -> MCPServer:
    store.members.update(seeded["org"].id, seeded["profile"].id, role)
    init_context(store, catalog)
    set_static_user(store.profiles.get(seeded["profile"].id))
    return create_mcp_server(McpSettings(), store=None)


WRITES = {
    "update_component",
    "delete_component",
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
    async with Client(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    by_name = {t.name: t for t in tools}
    assert set(by_name) == {t.name for t in TOOLS if not t.carries_secrets} | {"list_organisations"}
    assert "create_connections" not in by_name
    assert WRITES <= set(by_name)
    assert all(t.annotations is not None for t in tools)
    reads = {name for name, t in by_name.items() if t.annotations and t.annotations.read_only_hint}
    assert reads == set(by_name) - WRITES
    cancel, check = by_name["cancel_backfill"].annotations, by_name["check_connection"].annotations
    delete = by_name["delete_component"].annotations
    assert cancel is not None and cancel.destructive_hint is True
    assert delete is not None and delete.destructive_hint is True
    assert check is not None and check.open_world_hint is True


async def test_a_viewer_token_is_refused_on_writes(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded, role="viewer")) as client:
        result = _result_dict(
            await client.call_tool("toggle_job", {"component_id": str(seeded["job_id"]), "enabled": False})
        )

    assert result == {
        "status": "error",
        "error": "Requires editor role or higher",
        "valid_values": None,
        "category": None,
        "used_by": None,
    }
    assert (store.components.get(seeded["job_id"]).config or {})["enabled"] is True


async def test_an_editor_token_writes(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded)) as client:
        result = _result_dict(
            await client.call_tool("toggle_job", {"component_id": str(seeded["job_id"]), "enabled": False})
        )

    assert result["status"] == "success"
    assert (store.components.get(seeded["job_id"]).config or {})["enabled"] is False


async def test_list_jobs_returns_seeded_job_scoped_to_org(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded)) as client:
        result = _result_dict(await client.call_tool("list_jobs", {}))

    assert result["status"] == "success"
    assert result["count"] == 1
    assert result["jobs"][0]["key"] == "daily_sync"  # the other org's job is invisible


async def test_list_recent_runs_returns_seeded_run(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded)) as client:
        result = _result_dict(await client.call_tool("list_recent_runs", {"status": "success"}))

    assert result["status"] == "success"
    assert result["count"] == 1
    assert result["runs"][0]["component_id"] == str(seeded["job_id"])


async def test_tool_errors_are_structured_not_raised(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded)) as client:
        result = _result_dict(await client.call_tool("run_stats", {"component_id": "not-a-uuid"}))

    assert result["status"] == "error"
    assert "error" in result


async def test_tools_declare_output_schemas(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    missing = [t.name for t in tools if not t.output_schema]
    assert missing == []
    # Spot-check a computed shape made it into the schema.
    job_health = next(t for t in tools if t.name == "job_health")
    assert "JobHealthRow" in str(job_health.output_schema)


async def test_every_toolkit_tool_takes_an_optional_organisation(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded)) as client:
        tools = (await client.list_tools()).tools

    toolkit = [t for t in tools if t.name != "list_organisations"]
    assert all("organisation" in t.input_schema["properties"] for t in toolkit)
    assert not any("organisation" in t.input_schema.get("required", []) for t in toolkit)
    assert all("list_organisations" in t.input_schema["properties"]["organisation"]["description"] for t in toolkit)


async def test_a_named_organisation_scopes_the_call(store: Store, catalog: il.Catalog, seeded: dict):
    store.organisations.create(name="Globex", creator_id=seeded["profile"].id)

    async with Client(_server(store, catalog, seeded)) as client:
        omitted = _result_dict(await client.call_tool("list_jobs", {}))
        named = _result_dict(await client.call_tool("list_jobs", {"organisation": str(seeded["org"].id)}))

    assert omitted["status"] == "error"
    assert omitted["valid_values"] is not None and str(seeded["org"].id) in omitted["valid_values"]
    assert named["status"] == "success"
    assert [job["key"] for job in named["jobs"]] == ["daily_sync"]


async def test_list_organisations_lists_memberships_with_roles(store: Store, catalog: il.Catalog, seeded: dict):
    async with Client(_server(store, catalog, seeded, role="viewer")) as client:
        result = _result_dict(await client.call_tool("list_organisations", {}))

    assert result == {
        "status": "success",
        "organisations": [{"id": str(seeded["org"].id), "name": "Acme", "role": "viewer"}],
    }
