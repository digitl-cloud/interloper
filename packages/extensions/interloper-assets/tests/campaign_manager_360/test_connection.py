"""Regression tests for the CampaignManager360 connection.

Its profile lookup and ``check()`` run in the API process, which installs no
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

from interloper_assets.campaign_manager_360 import constants
from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection
from interloper_assets.google.auth import ServiceAccountAuth

CLIENT_EMAIL = "reporter@project.iam.gserviceaccount.com"


def _connection(status: int, body: dict[str, Any]) -> tuple[CampaignManager360Connection, list[tuple[str, str]]]:
    requests: list[tuple[str, str]] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append((request.method, str(request.url)))
        return httpx2.Response(status, json=body)

    connection = CampaignManager360Connection(service_account_key="{}")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    return connection, requests


class TestClient:
    def test_authenticates_as_the_service_account_for_reporting_and_trafficking(self):
        connection = CampaignManager360Connection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL}))
        client = connection.client

        assert str(client.base_url).rstrip("/") == "https://dfareporting.googleapis.com/dfareporting/v5"
        assert isinstance(client.auth, ServiceAccountAuth)
        assert (client.auth._client_id, client.auth._scope) == (CLIENT_EMAIL, " ".join(constants.SCOPES))
        assert connection.client is client


class TestApi:
    def test_the_api_is_cached_and_built_on_the_client(self):
        connection, _ = _connection(200, {})

        assert connection.api is connection.api
        assert connection.api.client is connection.client


class TestProfiles:
    async def test_lists_every_profile_with_its_account_in_one_request(self):
        connection, requests = _connection(
            200,
            {
                "items": [
                    {"profileId": "111", "accountId": "222", "userName": "reporting", "accountName": "Digitl"},
                    {"profileId": 333, "accountId": 444},
                ]
            },
        )

        assert await connection.profiles() == [
            {
                "profile_id": "111",
                "account_id": "222",
                "name": "reporting (Digitl)",
                "account_name": "Digitl",
            },
            {"profile_id": "333", "account_id": "444", "name": "333 (444)", "account_name": "444"},
        ]
        assert requests == [("GET", "https://dfareporting.googleapis.com/dfareporting/v5/userprofiles")]

    async def test_no_profiles(self):
        connection, _ = _connection(200, {})

        assert await connection.profiles() == []

    async def test_runs_without_the_sdk_extras(self, monkeypatch: pytest.MonkeyPatch):
        for module in ("bingads", "facebook_business"):
            monkeypatch.setitem(sys.modules, module, None)
        connection, _ = _connection(200, {"items": [{"profileId": "111", "accountId": "222"}]})

        assert await connection.profiles() == [
            {"profile_id": "111", "account_id": "222", "name": "111 (222)", "account_name": "222"}
        ]


class TestCheck:
    async def test_runs_the_lookup(self):
        connection, requests = _connection(200, {})

        assert await connection.check() is True
        assert len(requests) == 1

    async def test_a_refusal_reads_as_rejected_credentials(self):
        connection, _ = _connection(403, {"error": {"code": 403, "message": "The service account has no profile"}})

        result = await connection.verify(timeout=5)

        assert (result.ok, result.category) == (False, "auth")


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert (
            CampaignManager360Connection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL})).client.retry
            == il.HttpRetryPolicy()
        )
