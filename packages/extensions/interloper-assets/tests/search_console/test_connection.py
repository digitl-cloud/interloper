"""Regression tests for the SearchConsole connection.

Its property lookup and ``check()`` run in the API process, which installs no
vendor SDK extra, so they go over plain HTTP through the engine like the
assets do.
"""

from __future__ import annotations

import json
import sys
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.google.auth import ServiceAccountAuth
from interloper_assets.search_console import constants
from interloper_assets.search_console.connection import SearchConsoleConnection

CLIENT_EMAIL = "reader@project.iam.gserviceaccount.com"


def _connection(status: int, body: dict[str, Any]) -> tuple[SearchConsoleConnection, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(status, json=body)

    connection = SearchConsoleConnection(service_account_key="{}")
    connection.__dict__["client"] = il.AsyncRESTClient(
        constants.BASE_URL, transport=httpx2.MockTransport(handler), retry=il.RetryPolicy(max_attempts=1)
    )
    return connection, requests


class TestClient:
    def test_authenticates_as_the_service_account_with_the_read_only_scopes(self):
        connection = SearchConsoleConnection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL}))
        client = connection.client

        assert str(client.base_url).rstrip("/") == "https://searchconsole.googleapis.com"
        assert isinstance(client.auth, ServiceAccountAuth)
        assert (client.auth._client_id, client.auth._scope) == (CLIENT_EMAIL, " ".join(constants.SCOPES))
        assert connection.client is client


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection, _ = _connection(200, {})

        assert connection.api.client is connection.client
        assert connection.api is connection.api


class TestSites:
    async def test_lists_every_property_with_its_permission_level(self):
        connection, requests = _connection(
            200,
            {
                "siteEntry": [
                    {"siteUrl": "sc-domain:example.com", "permissionLevel": "SITE_OWNER"},
                    {"siteUrl": "https://example.org/", "permissionLevel": "SITE_FULL_USER"},
                ]
            },
        )

        assert await connection.sites() == [
            {"site_url": "sc-domain:example.com", "name": "sc-domain:example.com (SITE_OWNER)"},
            {"site_url": "https://example.org/", "name": "https://example.org/ (SITE_FULL_USER)"},
        ]
        assert [(r.method, str(r.url)) for r in requests] == [
            ("GET", "https://searchconsole.googleapis.com/webmasters/v3/sites")
        ]

    async def test_no_properties(self):
        connection, _ = _connection(200, {})

        assert await connection.sites() == []

    async def test_runs_without_the_sdk_extras(self, monkeypatch: pytest.MonkeyPatch):
        for module in ("bingads", "facebook_business", "google.ads"):
            monkeypatch.setitem(sys.modules, module, None)
        connection, _ = _connection(200, {"siteEntry": [{"siteUrl": "sc-domain:example.com"}]})

        assert await connection.sites() == [
            {"site_url": "sc-domain:example.com", "name": "sc-domain:example.com (UNKNOWN)"}
        ]


class TestCheck:
    async def test_runs_the_lookup(self):
        connection, requests = _connection(200, {})

        assert await connection.check() is True
        assert len(requests) == 1

    async def test_a_refusal_reads_as_rejected_credentials(self):
        connection, _ = _connection(403, {"error": {"code": 403, "message": "Permission denied"}})

        result = await connection.verify(timeout=5)

        assert (result.ok, result.category) == (False, "auth")
