"""Bearer-auth behaviour of the streamable HTTP transport.

The properties under test: no token and bad tokens are rejected with 401
before any tool machinery runs; a valid PAT reaches the MCP endpoint.
"""

from __future__ import annotations

from typing import Any

import httpx2
import interloper as il
from interloper.settings import McpSettings
from interloper_db.store import Store

from interloper_mcp.context import init_context
from interloper_mcp.server import create_mcp_server, http_options

INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0"},
    },
}

HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


async def _post(app: Any, json: dict, headers: dict[str, str]) -> httpx2.Response:
    # The session manager only runs inside the app lifespan.
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post("/mcp", json=json, headers=headers)


def _app(store: Store, catalog: il.Catalog, settings: McpSettings | None = None) -> Any:
    settings = settings or McpSettings()
    init_context(store, catalog)
    return create_mcp_server(settings, store=store).streamable_http_app(**http_options(settings))


async def test_missing_token_is_401(store: Store, catalog: il.Catalog, seeded: dict):
    resp = await _post(_app(store, catalog), INITIALIZE, HEADERS)
    assert resp.status_code == 401
    assert "www-authenticate" in resp.headers


async def test_invalid_token_is_401(store: Store, catalog: il.Catalog, seeded: dict):
    headers = {**HEADERS, "Authorization": "Bearer ilp_not-a-real-token"}
    resp = await _post(_app(store, catalog), INITIALIZE, headers)
    assert resp.status_code == 401


async def test_valid_token_initializes(store: Store, catalog: il.Catalog, seeded: dict):
    headers = {**HEADERS, "Authorization": f"Bearer {seeded['token']}"}
    resp = await _post(_app(store, catalog), INITIALIZE, headers)
    assert resp.status_code == 200
    assert '"serverInfo"' in resp.text


async def test_anonymous_request_advertises_resource_metadata(store: Store, catalog: il.Catalog, seeded: dict):
    settings = McpSettings(external_url="https://mcp.example.com/mcp")
    app = _app(store, catalog, settings)
    resp = await _post(app, INITIALIZE, HEADERS)
    assert resp.status_code == 401
    assert (
        'resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp"'
        in resp.headers["www-authenticate"]
    )

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://testserver") as client:
        metadata = await client.get("/.well-known/oauth-protected-resource/mcp")
    assert metadata.status_code == 200
    assert metadata.json()["resource"] == "https://mcp.example.com/mcp"
