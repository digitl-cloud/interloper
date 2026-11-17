"""Tests for the Amazon Selling Partner connection.

The region selects both the API host and the Login with Amazon token host.
The API reads the access token from ``x-amz-access-token``, not from
``Authorization``, and a refused token is exchanged again once. No
vendor-accessible call lists a connection's marketplaces, so the marketplace
picker is a static list per region, and the check proves the credentials by
exchanging the refresh token.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import parse_qs

import httpx2
import interloper as il
import pytest

from interloper_assets.amazon_selling_partner.connection import AmazonSellingPartnerConnection, LWATokenAuth

UNPATCHED_CLIENT = httpx2.AsyncClient


def _connection(location: str = "EU") -> AmazonSellingPartnerConnection:
    """Build a connection in a region.

    Args:
        location: The region's code.

    Returns:
        The connection, with placeholder credentials.
    """
    return AmazonSellingPartnerConnection(
        location=location, client_id="client-id", client_secret="client-secret", refresh_token="refresh-token"
    )


def _serve(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx2.Request], httpx2.Response]) -> None:
    """Answer every request of a client the connection builds itself with ``handler``.

    Args:
        monkeypatch: Replaces ``httpx2.AsyncClient`` for the test only.
        handler: The answer to each request.
    """
    monkeypatch.setattr(
        httpx2, "AsyncClient", lambda **kwargs: UNPATCHED_CLIENT(transport=httpx2.MockTransport(handler), **kwargs)
    )


def _token(_: httpx2.Request) -> httpx2.Response:
    """Answer a token exchange as Login with Amazon does.

    Args:
        _: The token request.

    Returns:
        A fresh access token.
    """
    return httpx2.Response(200, json={"access_token": "access-token", "token_type": "bearer", "expires_in": 3600})


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client


class TestClient:
    @pytest.mark.parametrize(
        ("location", "api_url", "auth_url"),
        [
            ("EU", "https://sellingpartnerapi-eu.amazon.com", "https://api.amazon.co.uk"),
            ("NA", "https://sellingpartnerapi-na.amazon.com", "https://api.amazon.com"),
            ("FE", "https://sellingpartnerapi-fe.amazon.com", "https://api.amazon.co.jp"),
        ],
    )
    def test_the_region_selects_the_api_and_token_hosts(self, location: str, api_url: str, auth_url: str):
        client = _connection(location).client

        assert str(client.base_url) == api_url
        assert isinstance(client.auth, LWATokenAuth)
        assert client.auth._token_request().url == f"{auth_url}/auth/o2/token"
        assert client.headers["user-agent"] == "interloper (Language=Python)"
        assert client.timeout == httpx2.Timeout(60)

    async def test_the_access_token_travels_in_the_amazon_header(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.path == "/auth/o2/token":
                return _token(request)
            return httpx2.Response(200, json={})

        auth = LWATokenAuth(
            base_url="https://api.amazon.co.uk",
            token_endpoint="/auth/o2/token",
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
        )
        async with UNPATCHED_CLIENT(auth=auth, transport=httpx2.MockTransport(handler)) as client:
            await client.get("https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/R1")

        token_request, api_request = requests
        assert parse_qs(token_request.content.decode()) == {
            "client_id": ["client-id"],
            "client_secret": ["client-secret"],
            "grant_type": ["refresh_token"],
            "refresh_token": ["refresh-token"],
        }
        assert api_request.headers["x-amz-access-token"] == "access-token"
        assert "authorization" not in api_request.headers

    @pytest.mark.parametrize("status", [401, 403])
    async def test_a_refused_token_is_exchanged_again_once(self, status: int):
        paths: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            paths.append(request.url.path)
            if request.url.path == "/auth/o2/token":
                return _token(request)
            return httpx2.Response(status if paths.count(request.url.path) == 1 else 200, json={})

        auth = LWATokenAuth(
            base_url="https://api.amazon.co.uk",
            token_endpoint="/auth/o2/token",
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
        )
        async with UNPATCHED_CLIENT(auth=auth, transport=httpx2.MockTransport(handler)) as client:
            response = await client.get("https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/R1")

        assert response.status_code == 200
        assert paths == ["/auth/o2/token", "/reports/2021-06-30/reports/R1"] * 2


class TestMarketplaces:
    async def test_lists_the_regions_marketplaces_without_a_request(self, monkeypatch: pytest.MonkeyPatch):
        def refuse(request: httpx2.Request) -> httpx2.Response:
            raise AssertionError(f"unexpected request {request.url}")

        _serve(monkeypatch, refuse)

        assert await _connection("FE").marketplaces() == [
            {"label": "Japan", "value": "A1VC38T7YXB528"},
            {"label": "Australia", "value": "A39IBJ37TRP1C6"},
            {"label": "Singapore", "value": "A19VAU5U5O7RUS"},
        ]


class TestCheck:
    async def test_exchanges_the_refresh_token_at_the_regions_token_host(self, monkeypatch: pytest.MonkeyPatch):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _token(request)

        _serve(monkeypatch, handler)

        assert await _connection("NA").check() is True
        (request,) = requests
        assert (request.method, str(request.url)) == ("POST", "https://api.amazon.com/auth/o2/token")
        assert parse_qs(request.content.decode()) == {
            "grant_type": ["refresh_token"],
            "refresh_token": ["refresh-token"],
            "client_id": ["client-id"],
            "client_secret": ["client-secret"],
        }
        assert request.extensions["timeout"] == {"connect": 30, "read": 30, "write": 30, "pool": 30}

    async def test_a_refused_refresh_token_fails_the_check(self, monkeypatch: pytest.MonkeyPatch):
        _serve(monkeypatch, lambda _: httpx2.Response(400, json={"error": "invalid_grant"}))

        result = await _connection().verify(timeout=5)

        assert result == il.CheckResult(
            ok=False, live=True, category="error", message="The provider responded with HTTP 400."
        )
