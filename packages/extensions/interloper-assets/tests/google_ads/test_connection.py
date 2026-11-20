"""Regression tests for the Google Ads connection.

The connection's client speaks the Google Ads REST API: it trades the refresh
token for an access token at Google's token endpoint and sends the developer
token on every request. The ``customers`` lookup goes through the engine as the
assets do: it lists the accessible customers and names each one with a one-row
GAQL search, falling back to the bare id for a customer that refuses the query.
These tests drive the connection's own client, its token exchange included,
against a fake transport.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx2
import interloper as il
import pytest
from interloper.errors import ConnectionCheckError

from interloper_assets.google_ads.connection import GoogleAdsConnection
from interloper_assets.google_ads.constants import API_VERSION, BASE_URL


def _refusal(status: int, name: str, code: dict[str, str], message: str) -> httpx2.Response:
    failure = {"@type": f"type.googleapis.com/google.ads.googleads.{API_VERSION}.errors.GoogleAdsFailure"}
    failure["errors"] = [{"errorCode": code, "message": message}]
    error = {"code": status, "message": "The request was refused.", "status": name, "details": [failure]}
    return httpx2.Response(status, json={"error": error})


def _connection(handler: Callable[[httpx2.Request], httpx2.Response] | None = None) -> GoogleAdsConnection:
    connection = GoogleAdsConnection(
        client_id="cid", client_secret="secret", refresh_token="refresh", developer_token="dev"
    )
    if handler is not None:
        connection.client._transport = httpx2.MockTransport(handler)
    return connection


def _token_or(handler: Callable[[httpx2.Request], httpx2.Response]) -> Callable[[httpx2.Request], httpx2.Response]:
    def serve(request: httpx2.Request) -> httpx2.Response:
        if request.url.host == "oauth2.googleapis.com":
            return httpx2.Response(200, json={"access_token": "access"})
        return handler(request)

    return serve


class TestClient:
    def test_speaks_the_pinned_rest_api_version_with_the_developer_token(self):
        client = _connection().client

        assert str(client.base_url) == f"https://googleads.googleapis.com/{API_VERSION}/"
        assert client.headers["developer-token"] == "dev"
        assert isinstance(client.auth, il.OAuth2RefreshTokenAuth)

    def test_repeats_a_failed_search_like_a_get(self):
        assert _connection().client.retry == il.HttpRetryPolicy(idempotent_methods={"GET", "POST"})

    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client
        assert connection.api is connection.api


class TestCustomers:
    def _handler(self, requests: list[httpx2.Request]) -> Callable[[httpx2.Request], httpx2.Response]:
        streams = {
            "111": httpx2.Response(200, json=[{"results": [{"customer": {"id": "111", "descriptiveName": "Acme"}}]}]),
            "222": httpx2.Response(403, json={"error": "suspended"}),
            "333": httpx2.Response(200, json=[{"results": [{"customer": {"id": "333"}}]}]),
        }

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.host == "oauth2.googleapis.com":
                return httpx2.Response(200, json={"access_token": "access"})
            if request.url.path.endswith("customers:listAccessibleCustomers"):
                return httpx2.Response(200, json={"resourceNames": ["customers/111", "customers/222", "customers/333"]})
            return streams[request.url.path.split("/")[-2]]

        return handler

    async def test_names_every_accessible_customer(self):
        requests: list[httpx2.Request] = []
        assert await _connection(self._handler(requests)).customers() == [
            {"customer_id": "111", "name": "Acme"},
            {"customer_id": "222", "name": "222"},
            {"customer_id": "333", "name": "333"},
        ]

        token_request, list_request, *search_requests = requests
        assert str(token_request.url) == "https://oauth2.googleapis.com/token"
        assert parse_qs(token_request.content.decode()) == {
            "grant_type": ["refresh_token"],
            "refresh_token": ["refresh"],
            "client_id": ["cid"],
            "client_secret": ["secret"],
        }
        assert str(list_request.url) == f"{BASE_URL}/customers:listAccessibleCustomers"
        assert [str(r.url) for r in search_requests] == [
            f"{BASE_URL}/customers/{customer_id}/googleAds:searchStream" for customer_id in ("111", "222", "333")
        ]
        for request in (list_request, *search_requests):
            assert request.headers["Authorization"] == "Bearer access"
            assert request.headers["developer-token"] == "dev"
        assert json.loads(search_requests[0].content) == {
            "query": "SELECT customer.id, customer.descriptive_name, customer.status FROM customer LIMIT 1"
        }

    @pytest.mark.parametrize("listing", [{}, {"resourceNames": []}])
    async def test_credentials_reaching_no_customer_fail(self, listing: dict[str, Any]):
        connection = _connection(_token_or(lambda request: httpx2.Response(200, json=listing)))

        with pytest.raises(ConnectionCheckError, match="Google Ads lists no customer these credentials can access"):
            await connection.customers()

    async def test_a_refused_listing_fails_with_googles_reason(self):
        refusal = _refusal(401, "UNAUTHENTICATED", {"authenticationError": "NOT_ADS_USER"}, "Not a valid Ads user.")
        connection = _connection(_token_or(lambda request: refusal))

        with pytest.raises(ConnectionCheckError, match=r"UNAUTHENTICATED\): Not a valid Ads user\.$"):
            await connection.customers()

    async def test_every_customer_refusing_its_name_fails_with_googles_reason(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("customers:listAccessibleCustomers"):
                return httpx2.Response(200, json={"resourceNames": ["customers/111", "customers/222"]})
            code = {"authorizationError": "DEVELOPER_TOKEN_NOT_APPROVED"}
            return _refusal(403, "PERMISSION_DENIED", code, "Test accounts only.")

        with pytest.raises(ConnectionCheckError, match=r"PERMISSION_DENIED\): Test accounts only\.$"):
            await _connection(_token_or(handler)).check()

    async def test_check_runs_the_lookup(self):
        assert await _connection(self._handler([])).check() is True

    async def test_a_rejected_refresh_token_fails_with_googles_reason(self):
        rejection = {"error": "invalid_grant", "error_description": "Token has been expired or revoked."}
        connection = _connection(lambda request: httpx2.Response(400, json=rejection))

        with pytest.raises(ConnectionCheckError, match=r"invalid_grant\): Token has been expired or revoked\.$"):
            await connection.check()
