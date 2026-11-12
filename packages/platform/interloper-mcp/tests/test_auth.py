"""Bearer-auth behaviour of the streamable HTTP transport.

The properties under test: no token, bad tokens and tokens of a kind or
audience MCP does not accept are rejected with 401 before any tool machinery
runs; a valid PAT reaches the MCP endpoint and its tools act as its holder;
the resource the server advertises is the one the authorization server
issues tokens for.
"""

from __future__ import annotations

from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.settings import AppSettings, McpSettings, SecretsSettings, ServerSettings
from interloper_api.authorization import AuthorizationServer
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

PUBLIC_URL = "https://app.example.com"


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


def _app(store: Store, catalog: il.Catalog, public_url: str = PUBLIC_URL) -> Any:
    init_context(store, catalog)
    return create_mcp_server(public_url, store=store).streamable_http_app(**http_options(McpSettings()))


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
    app = _app(store, catalog)
    resp = await _post(app, INITIALIZE, HEADERS)
    assert resp.status_code == 401
    assert (
        'resource_metadata="https://app.example.com/.well-known/oauth-protected-resource/mcp"'
        in resp.headers["www-authenticate"]
    )

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://testserver") as client:
        metadata = await client.get("/.well-known/oauth-protected-resource/mcp")
    assert metadata.status_code == 200
    assert metadata.json()["resource"] == "https://app.example.com/mcp"


@pytest.mark.parametrize(
    ("external_url", "public_url"),
    [("https://app.example.com/", "https://app.example.com"), ("", "http://localhost:3100")],
    ids=["external-url", "local-server"],
)
async def test_a_client_signs_in_to_the_resource_the_server_advertises(
    store: Store, catalog: il.Catalog, seeded: dict, external_url: str, public_url: str
):
    settings = AppSettings(
        server=ServerSettings(external_url=external_url, port=3100),
        secrets=SecretsSettings(encryption_key="key-material"),
    )
    authorization_server = AuthorizationServer.from_settings(store, settings)
    app = _app(store, catalog, settings.server.public_url)

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://testserver") as client:
        metadata = (await client.get("/.well-known/oauth-protected-resource/mcp")).json()
    audience = authorization_server.resolve_resource([metadata["resource"]])
    access_token = store.tokens.create_access(seeded["profile"].id, "a-client", audience)
    resp = await _post(app, INITIALIZE, {**HEADERS, "Authorization": f"Bearer {access_token}"})

    assert metadata["resource"] == f"{public_url}/mcp"
    assert metadata["authorization_servers"] == [authorization_server.metadata["issuer"]] == [public_url]
    assert audience is Audience.MCP
    assert resp.status_code == 200


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
