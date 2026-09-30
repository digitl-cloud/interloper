"""Regression tests for the Google Ads connection.

The SDK client is built from the connection's OAuth trio plus its developer
token. The ``customers`` lookup runs over plain REST instead: it trades the
refresh token for an access token, lists the accessible customers and names
each one with a one-row GAQL search, falling back to the bare id for a customer
that refuses the query. These tests drive both against fakes.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx2
import pytest
from google.ads.googleads.client import GoogleAdsClient

from interloper_assets.google_ads.connection import GoogleAdsConnection
from interloper_assets.google_ads.constants import API_VERSION

BASE_URL = f"https://googleads.googleapis.com/{API_VERSION}"


def _connection() -> GoogleAdsConnection:
    return GoogleAdsConnection(client_id="cid", client_secret="secret", refresh_token="refresh", developer_token="dev")


def _serve(monkeypatch: Any, handler: Callable[[httpx2.Request], httpx2.Response]) -> None:
    real_client = httpx2.AsyncClient
    monkeypatch.setattr(
        httpx2, "AsyncClient", lambda **kwargs: real_client(transport=httpx2.MockTransport(handler), **kwargs)
    )


class TestClient:
    def test_loads_the_sdk_client_from_the_credentials(self, monkeypatch: Any):
        configs: list[dict[str, Any]] = []
        sentinel = object()

        def load_from_dict(config: dict[str, Any]) -> object:
            configs.append(config)
            return sentinel

        monkeypatch.setattr(GoogleAdsClient, "load_from_dict", load_from_dict)
        connection = _connection()
        assert connection.client is sentinel
        assert connection.client is sentinel
        assert configs == [
            {
                "client_id": "cid",
                "client_secret": "secret",
                "refresh_token": "refresh",
                "developer_token": "dev",
                "use_proto_plus": True,
                "api_version": API_VERSION,
            }
        ]


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

    async def test_names_every_accessible_customer(self, monkeypatch: Any):
        requests: list[httpx2.Request] = []
        _serve(monkeypatch, self._handler(requests))
        assert await _connection().customers() == [
            {"customer_id": "111", "name": "Acme (111)"},
            {"customer_id": "222", "name": "222"},
            {"customer_id": "333", "name": "333 (333)"},
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

    async def test_no_accessible_customers(self, monkeypatch: Any):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.host == "oauth2.googleapis.com":
                return httpx2.Response(200, json={"access_token": "access"})
            return httpx2.Response(200, json={})

        _serve(monkeypatch, handler)
        assert await _connection().customers() == []

    async def test_check_runs_the_lookup(self, monkeypatch: Any):
        _serve(monkeypatch, self._handler([]))
        assert await _connection().check() is True

    async def test_check_raises_on_a_rejected_refresh_token(self, monkeypatch: Any):
        _serve(monkeypatch, lambda request: httpx2.Response(400, json={"error": "invalid_grant"}))
        with pytest.raises(httpx2.HTTPStatusError):
            await _connection().check()
