"""Tests for the LinkedIn Ads connection.

The client speaks a pinned Marketing API version over Rest.li 2.0. The account
dropdown pages the ``q=search`` finder by ``start``/``count`` until an empty
page, and the connection check is that lookup: LinkedIn refuses a dead token
with HTTP 401 and its own code and message, which the check reports.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il

from interloper_assets.linkedin_ads import constants
from interloper_assets.linkedin_ads.connection import LinkedinAdsConnection

ACCOUNT_PAGES = {
    "0": [{"id": 1, "name": "Alpha"}, {"id": 2}],
    "100": [{"id": 3, "name": "Gamma"}],
    "200": [],
}


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> LinkedinAdsConnection:
    connection = LinkedinAdsConnection(client_id="cid", client_secret="secret", refresh_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    return connection


def _accounts(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(200, json={"elements": ACCOUNT_PAGES[request.url.params["start"]]})


class TestClient:
    def test_the_client_speaks_the_pinned_api_version_over_restli_2(self):
        client = LinkedinAdsConnection(client_id="cid", client_secret="secret", refresh_token="token").client

        assert str(client.base_url) == "https://api.linkedin.com/rest/"
        assert client.headers["LinkedIn-Version"] == "202607"
        assert client.headers["X-Restli-Protocol-Version"] == "2.0.0"
        assert client.headers["Accept-Encoding"] == "gzip, deflate, br"
        assert isinstance(client.auth, il.OAuth2RefreshTokenAuth)


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(_accounts)

        assert connection.api.client is connection.client
        assert connection.api is connection.api


class TestAccounts:
    async def test_lists_every_account_page_by_page(self):
        sent: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(f"{request.method} {request.url.path}?{request.url.query.decode()}")
            return _accounts(request)

        assert await _connection(handler).accounts() == [
            {"id": "1", "name": "Alpha"},
            {"id": "2", "name": "2"},
            {"id": "3", "name": "Gamma"},
        ]
        assert sent == [
            "GET /rest/adAccounts?q=search&count=100&start=0",
            "GET /rest/adAccounts?q=search&count=100&start=100",
            "GET /rest/adAccounts?q=search&count=100&start=200",
        ]


class TestCheck:
    async def test_a_working_token_passes(self):
        assert await _connection(_accounts).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_an_account_list_without_elements_fails_the_check(self):
        connection = _connection(lambda _: httpx2.Response(200, json={}))

        assert await connection.verify(timeout=5) == il.CheckResult(
            ok=False, live=True, category="error", message="The connection check failed unexpectedly."
        )

    async def test_a_refused_token_reports_linkedins_reason(self):
        refused = {"status": 401, "serviceErrorCode": 65601, "code": "REVOKED_ACCESS_TOKEN", "message": "revoked"}
        connection = _connection(lambda _: httpx2.Response(401, json=refused))

        assert await connection.verify(timeout=5) == il.CheckResult(
            ok=False,
            live=True,
            category="error",
            message="LinkedIn refused the request (HTTP 401, REVOKED_ACCESS_TOKEN, service error 65601): revoked",
        )
