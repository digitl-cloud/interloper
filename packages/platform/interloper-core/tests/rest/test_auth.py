"""Tests for ``interloper.rest.auth``."""

from __future__ import annotations

import httpx2
import pytest

from interloper.rest.auth import HTTPBearerAuth, OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth, OAuth2TokenError
from interloper.rest.client import AsyncRESTClient, RESTClient


def _oauth_handler(*, expect_grant: str, token: str = "tok-1"):
    """A transport that issues a token at /oauth2/token and 401s any other bearer.

    Records every request so tests can assert the token exchange went through the
    *active* client (not a private side client).

    Returns:
        The handler and the list it appends every seen request to.
    """
    seen: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        if request.url.path == "/oauth2/token":
            form = dict(httpx2.QueryParams(request.content.decode()))
            assert form["grant_type"] == expect_grant
            return httpx2.Response(200, json={"access_token": token, "refresh_token": "rt-new"})
        if request.headers.get("Authorization") != f"Bearer {token}":
            return httpx2.Response(401, json={"error": "expired"})
        return httpx2.Response(200, json={"ok": True})

    return handler, seen


def _echo_authorization(request: httpx2.Request) -> httpx2.Response:
    """A transport that echoes the request's ``Authorization`` header back as JSON.

    Args:
        request: The intercepted request.

    Returns:
        A 200 response whose body carries the header under ``auth``.
    """
    return httpx2.Response(200, json={"auth": request.headers.get("Authorization")})


class TestHTTPBearerAuth:
    """One ``auth_flow`` must serve the sync and the async client alike."""

    def test_sync_client_sends_bearer_header(self):
        auth = HTTPBearerAuth("t")
        client = RESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(_echo_authorization))
        with client:
            assert client.get("/data").json() == {"auth": "Bearer t"}

    async def test_async_client_sends_bearer_header(self):
        auth = HTTPBearerAuth("t")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(_echo_authorization))
        async with client:
            response = await client.get("/data")
        assert response.json() == {"auth": "Bearer t"}


class TestOAuth2SyncClient:
    def test_acquires_token_via_the_active_client(self):
        handler, seen = _oauth_handler(expect_grant="client_credentials")
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret")
        client = RESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))
        with client:
            response = client.get("/data")
        assert response.json() == {"ok": True}
        # The token exchange ran through this client's transport.
        assert [r.url.path for r in seen] == ["/oauth2/token", "/data"]
        assert auth.access_token == "tok-1"


class TestOAuth2AsyncClient:
    async def test_acquires_token_natively(self):
        handler, seen = _oauth_handler(expect_grant="refresh_token")
        auth = OAuth2RefreshTokenAuth("https://api.test", "cid", "secret", refresh_token="rt-0")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))
        async with client:
            response = await client.get("/data")
        assert response.json() == {"ok": True}
        assert [r.url.path for r in seen] == ["/oauth2/token", "/data"]

    async def test_refreshes_on_401(self):
        handler, seen = _oauth_handler(expect_grant="client_credentials")
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret", access_token="stale")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))
        async with client:
            response = await client.get("/data")
        assert response.json() == {"ok": True}
        # stale bearer → 401 → token exchange → retry succeeds.
        assert [r.url.path for r in seen] == ["/data", "/oauth2/token", "/data"]
        assert auth.access_token == "tok-1"

    async def test_clear_token_forces_reacquire(self):
        handler, seen = _oauth_handler(expect_grant="client_credentials")
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret", access_token="tok-1")
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))
        async with client:
            await client.get("/data")  # uses the pre-seeded token, no exchange
            assert [r.url.path for r in seen] == ["/data"]
            auth.clear_token()
            await client.get("/data")  # now must re-acquire
        assert [r.url.path for r in seen] == ["/data", "/oauth2/token", "/data"]


def _refusing_token_endpoint(token_response: httpx2.Response):
    """A transport whose token endpoint answers *token_response* and whose API 401s a stale bearer.

    Returns:
        The handler.
    """

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/oauth2/token":
            return token_response
        return httpx2.Response(401, json={"error": "expired"})

    return handler


INVALID_GRANT = {
    "error": "invalid_grant",
    "error_description": "The provided authorization grant or refresh token is invalid, expired or revoked.",
}


class TestRefusedTokenRequest:
    async def test_raises_the_providers_error_and_description_without_a_credential(self):
        auth = OAuth2RefreshTokenAuth("https://api.test", "cid", "client-secret", refresh_token="rt-secret")
        handler = _refusing_token_endpoint(httpx2.Response(400, json=INVALID_GRANT))
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))

        with pytest.raises(OAuth2TokenError) as raised:
            async with client:
                await client.get("/data")

        assert str(raised.value) == (
            "The provider refused the access token request (HTTP 400, invalid_grant): "
            "The provided authorization grant or refresh token is invalid, expired or revoked."
        )
        assert raised.value.error == "invalid_grant"
        assert raised.value.response.status_code == 400
        assert "rt-secret" not in str(raised.value)
        assert "client-secret" not in str(raised.value)

    async def test_is_still_the_http_error_status(self):
        auth = OAuth2ClientCredentialsAuth("https://api.test", "cid", "secret", access_token="stale")
        handler = _refusing_token_endpoint(httpx2.Response(401, json={"error": "invalid_client"}))
        client = AsyncRESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            async with client:
                await client.get("/data")

        assert (
            str(raised.value)
            == "The provider refused the access token request (HTTP 401, invalid_client): Unauthorized"
        )
        assert raised.value.response.status_code == 401

    @pytest.mark.parametrize("body", [{"text": "<html>bad gateway</html>"}, {"json": {"error": {"code": 5}}}])
    def test_a_body_that_is_not_an_oauth_error_gives_the_status(self, body: dict):
        auth = OAuth2RefreshTokenAuth("https://api.test", "cid", "secret", refresh_token="rt-0")
        handler = _refusing_token_endpoint(httpx2.Response(502, **body))
        client = RESTClient("https://api.test", auth=auth, transport=httpx2.MockTransport(handler))

        with pytest.raises(OAuth2TokenError) as raised, client:
            client.get("/data")

        assert str(raised.value) == "The provider refused the access token request (HTTP 502): Bad Gateway"
        assert raised.value.error is None
