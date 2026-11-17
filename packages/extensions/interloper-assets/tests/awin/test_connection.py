"""Tests for the Awin connection.

The advertiser lookup backs the source's ``advertiser_id`` field and the
connection check: one ``/accounts`` request filtered to advertisers, sorted by
name. Awin refuses a bad token with an HTTP error status.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il

from interloper_assets.awin.connection import AwinConnection
from interloper_assets.awin.constants import BASE_URL


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> AwinConnection:
    connection = AwinConnection(access_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        BASE_URL, auth=il.HTTPBearerAuth("token"), transport=httpx2.MockTransport(handler)
    )
    return connection


def _accounts(_: httpx2.Request) -> httpx2.Response:
    accounts = [
        {"accountId": 2, "accountName": "beta", "accountType": "advertiser"},
        {"accountId": 3, "accountName": None, "accountType": "advertiser"},
        {"accountId": 1, "accountName": "Alpha", "accountType": "advertiser"},
    ]
    return httpx2.Response(200, json={"userId": 9, "accounts": accounts})


class TestClient:
    def test_the_client_sends_the_token_as_its_bearer(self):
        connection = AwinConnection(access_token="token")
        request = connection.client.build_request("GET", "/accounts")
        auth = connection.client.auth
        assert auth is not None

        sent = next(auth.auth_flow(request))

        assert str(sent.url) == "https://api.awin.com/accounts"
        assert sent.headers["Authorization"] == "Bearer token"


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(_accounts)

        assert connection.api.client is connection.client


class TestAdvertisers:
    async def test_lists_the_advertisers_by_name(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _accounts(request)

        assert await _connection(handler).advertisers() == [
            {"advertiser_id": "3", "name": "3"},
            {"advertiser_id": "1", "name": "Alpha"},
            {"advertiser_id": "2", "name": "beta"},
        ]
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == "https://api.awin.com/accounts?type=advertiser"

    async def test_a_token_without_accounts_lists_none(self):
        assert await _connection(lambda _: httpx2.Response(200, json={"userId": 9})).advertisers() == []


class TestCheck:
    async def test_a_working_token_passes(self):
        assert await _connection(_accounts).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_token_fails_live(self):
        result = await _connection(lambda _: httpx2.Response(401, json={"error": "Unauthorized"})).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False, live=True, category="auth", message="The provider rejected the credentials."
        )
