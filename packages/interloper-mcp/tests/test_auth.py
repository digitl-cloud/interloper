"""Bearer-auth behaviour of the streamable HTTP transport.

The properties under test: no token, bad tokens and tokens of a kind or
audience MCP does not accept are rejected with 401 before any tool machinery
runs; a valid PAT reaches the MCP endpoint and its tools act as its holder.
"""

from __future__ import annotations

from typing import Any

import httpx2
import interloper as il
from interloper.settings import McpSettings
from interloper_db import engine as engine_module
from interloper_db.crypto import hash_token
from interloper_db.models import Audience, Profile, Token, TokenKind
from interloper_db.store import Store
from sqlmodel import Session

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


def _insert_token(profile: Profile, raw: str, kind: TokenKind, resource: str | None = None) -> None:
    # Nothing issues session rows or access tokens yet, so they are written directly.
    with Session(engine_module.get_engine()) as session:
        session.add(
            Token(user_id=profile.id, kind=kind, resource=resource, token_prefix=raw[:12], token_hash=hash_token(raw))
        )
        session.commit()


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


async def test_session_token_refused(store: Store, catalog: il.Catalog, seeded: dict):
    _insert_token(seeded["profile"], "ils_session-secret", TokenKind.SESSION)

    headers = {**HEADERS, "Authorization": "Bearer ils_session-secret"}
    resp = await _post(_app(store, catalog), INITIALIZE, headers)

    assert resp.status_code == 401


async def test_api_access_token_refused(store: Store, catalog: il.Catalog, seeded: dict):
    _insert_token(seeded["profile"], "ila_api-secret", TokenKind.ACCESS, resource=Audience.API.value)

    headers = {**HEADERS, "Authorization": "Bearer ila_api-secret"}
    resp = await _post(_app(store, catalog), INITIALIZE, headers)

    assert resp.status_code == 401


async def test_mcp_access_token_initializes(store: Store, catalog: il.Catalog, seeded: dict):
    _insert_token(seeded["profile"], "ila_mcp-secret", TokenKind.ACCESS, resource=Audience.MCP.value)

    headers = {**HEADERS, "Authorization": "Bearer ila_mcp-secret"}
    resp = await _post(_app(store, catalog), INITIALIZE, headers)

    assert resp.status_code == 200


async def test_a_tool_call_acts_as_the_token_holder(store: Store, catalog: il.Catalog, seeded: dict):
    call = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "list_jobs", "arguments": {}}}
    headers = {**HEADERS, "Authorization": f"Bearer {seeded['token']}"}

    resp = await _post(_app(store, catalog), call, headers)

    assert resp.status_code == 200
    assert "daily_sync" in resp.text
    assert "other_org_job" not in resp.text
