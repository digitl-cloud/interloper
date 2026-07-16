"""Bearer-auth behaviour of the streamable HTTP transport.

The properties under test: no token and bad tokens are rejected with 401
before any tool machinery runs; a valid PAT reaches the MCP endpoint.
"""

from __future__ import annotations

from typing import Any

import httpx
import interloper as il
from interloper.settings import McpSettings
from interloper_db.store import Store

from interloper_mcp.context import init_context
from interloper_mcp.server import create_mcp_server

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


async def _post(app: Any, json: dict, headers: dict[str, str]) -> httpx.Response:
    # The session manager only runs inside the app lifespan.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post("/mcp", json=json, headers=headers)


def _app(store: Store, catalog: il.Catalog) -> Any:
    init_context(store, catalog)
    return create_mcp_server(McpSettings(), store=store).streamable_http_app()


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
