"""Regression tests for the Search Ads 360 connection."""

from __future__ import annotations

import json
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.errors import ConnectionCheckError

from interloper_assets.google.auth import ServiceAccountAuth
from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.constants import BASE_URL, SCOPES

CLIENT_EMAIL = "reporter@project.iam.gserviceaccount.com"


def _connection() -> SearchAds360Connection:
    return SearchAds360Connection(service_account_key=json.dumps({"client_email": CLIENT_EMAIL}))


class TestClient:
    def test_targets_the_reporting_api(self):
        assert str(_connection().client.base_url).rstrip("/") == BASE_URL

    def test_authenticates_as_the_service_account_for_the_reporting_scope(self):
        auth = _connection().client.auth

        assert isinstance(auth, ServiceAccountAuth)
        assert (auth._client_id, auth._scope) == (CLIENT_EMAIL, " ".join(SCOPES))


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client


def _answering(response: httpx2.Response) -> tuple[SearchAds360Connection, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return response

    connection = _connection()
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return connection, requests


class TestCheck:
    async def test_lists_the_customers_the_service_account_reaches(self):
        connection, requests = _answering(httpx2.Response(200, json={"resourceNames": ["customers/111"]}))

        assert await connection.check() is True
        (request,) = requests
        assert (request.method, str(request.url)) == ("GET", f"{BASE_URL}/customers:listAccessibleCustomers")

    async def test_a_refused_lookup_fails_with_googles_reason(self):
        refusal = {"code": 403, "message": "The caller does not have permission", "status": "PERMISSION_DENIED"}
        connection, _ = _answering(httpx2.Response(403, json={"error": refusal}))

        with pytest.raises(ConnectionCheckError, match=r"PERMISSION_DENIED\): The caller does not have permission"):
            await connection.check()

    @pytest.mark.parametrize("body", [{}, {"resourceNames": []}])
    async def test_a_service_account_reaching_no_customer_fails(self, body: dict[str, Any]):
        connection, _ = _answering(httpx2.Response(200, json=body))

        with pytest.raises(ConnectionCheckError, match="lists no customer this service account can access"):
            await connection.check()


class TestRetry:
    def test_the_client_repeats_a_search_like_a_get_since_the_api_only_reads(self):
        assert _connection().client.retry == il.HttpRetryPolicy(idempotent_methods={"GET", "POST"})
