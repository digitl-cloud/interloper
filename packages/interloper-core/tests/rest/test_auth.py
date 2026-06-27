"""Tests for ``interloper.rest.auth``."""

from __future__ import annotations

import httpx

from interloper.rest.auth import OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth
from interloper.rest.client import AsyncRESTClient, RESTClient


def _oauth_handler(*, expect_grant: str, token: str = "tok-1"):
    """A transport that issues a token at /oauth2/token and 401s any other bearer.

    Records every request so tests can assert the token exchange went through the
    *active* client (not a private side client).

    Returns:
        The handler and the list it appends every seen request to.
    """
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/oauth2/token":
            form = dict(httpx.QueryParams(request.content.decode()))
            assert form["grant_type"] == expect_grant
            return httpx.Response(200, json={"access_token": token, "refresh_token": "rt-new"})
        if request.headers.get("Authorization") != f"Bearer {token}":
            return httpx.Response(401, json={"error": "expired"})
        return httpx.Response(200, json={"ok": True})

    return handler, seen


class TestOAuth2SyncClient:
    def test_acquires_token_via_the_active_client(self):
        handler, seen = _oauth_handler(expect_grant="client_credentials")
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret")
        client = RESTClient("https://api.test", auth=auth, transport=httpx.MockTransport(handler))
        with client:
            resp = client.get("/data")
        assert resp.json() == {"ok": True}
        # The token exchange ran through this client's transport.
        assert [r.url.path for r in seen] == ["/oauth2/token", "/data"]
        assert auth.access_token == "tok-1"


class TestOAuth2AsyncClient:
    async def test_acquires_token_natively(self):
        handler, seen = _oauth_handler(expect_grant="refresh_token")
        auth = OAuth2RefreshTokenAuth("https://api.test", "cid", "secret", refresh_token="rt-0")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx.MockTransport(handler))
        async with client:
            resp = await client.get("/data")
        assert resp.json() == {"ok": True}
        assert [r.url.path for r in seen] == ["/oauth2/token", "/data"]

    async def test_refreshes_on_401(self):
        handler, seen = _oauth_handler(expect_grant="client_credentials")
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret", access_token="stale")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx.MockTransport(handler))
        async with client:
            resp = await client.get("/data")
        assert resp.json() == {"ok": True}
        # stale bearer → 401 → token exchange → retry succeeds.
        assert [r.url.path for r in seen] == ["/data", "/oauth2/token", "/data"]
        assert auth.access_token == "tok-1"

    async def test_clear_token_forces_reacquire(self):
        handler, seen = _oauth_handler(expect_grant="client_credentials")
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret", access_token="tok-1")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx.MockTransport(handler))
        async with client:
            await client.get("/data")  # uses the pre-seeded token, no exchange
            assert [r.url.path for r in seen] == ["/data"]
            auth.clear_token()
            await client.get("/data")  # now must re-acquire
        assert [r.url.path for r in seen] == ["/data", "/oauth2/token", "/data"]
