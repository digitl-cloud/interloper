"""Tests for the Adtraction connection.

Adtraction authenticates with the API token in an ``X-Token`` header and
refuses a request without a JSON content type, a GET included. The connection
exposes that client as ``client``, the engine built on it as ``api``, and
checks the token by reading the advertiser account through ``api``.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.adtraction.connection import AdtractionConnection

BASE_URL = "https://api.adtraction.net/v2"


def _connection() -> AdtractionConnection:
    return AdtractionConnection(api_token="secret-token")


def _mocked(handler: Callable[[httpx2.Request], httpx2.Response]) -> AdtractionConnection:
    connection = _connection()
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return connection


class TestClient:
    def test_reads_the_v2_api_of_adtraction(self):
        request = _connection().client.build_request("GET", "/advertiser/account/")

        assert str(request.url) == "https://api.adtraction.net/v2/advertiser/account/"

    def test_sends_the_token_and_json_content_type_on_every_request(self):
        request = _connection().client.build_request("GET", "/advertiser/account/")

        assert request.headers["X-Token"] == "secret-token"
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["Accept"] == "application/json"

    def test_is_cached_per_connection(self):
        connection = _connection()

        assert connection.client is connection.client


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client
        assert connection.api is connection.api


class TestCheck:
    async def test_reads_the_advertiser_account(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"programId": 1, "programName": "Example", "currency": "SEK"})

        assert await _mocked(handler).check() is True
        (request,) = requests
        assert (request.method, str(request.url)) == ("GET", f"{BASE_URL}/advertiser/account/")

    async def test_a_refused_token_raises_its_http_status(self):
        connection = _mocked(lambda _: httpx2.Response(401, json={"message": "Unauthorized access", "status": 401}))

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await connection.check()


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert _connection().client.retry == il.HttpRetryPolicy()
