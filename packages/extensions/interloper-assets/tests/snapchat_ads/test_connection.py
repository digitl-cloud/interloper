"""Tests for the Snapchat Ads connection.

The ad account dropdown lists the active ad accounts of every organization the
token can see, one organization at a time. Snapchat refuses a request with an
HTTP error status, which the connection check reports by its category.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il

from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.constants import BASE_URL

ORGANIZATIONS = "https://adsapi.snapchat.com/v1/me/organizations"


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> SnapchatAdsConnection:
    connection = SnapchatAdsConnection(client_id="client-id", client_secret="secret", refresh_token="refresh")
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return connection


def _wrapped(list_key: str, item_key: str, records: list[dict[str, Any]]) -> httpx2.Response:
    items = [{"sub_request_status": "SUCCESS", item_key: record} for record in records]
    return httpx2.Response(200, json={"request_status": "SUCCESS", list_key: items})


def _organizations(request: httpx2.Request) -> httpx2.Response:
    if str(request.url) == ORGANIZATIONS:
        return _wrapped("organizations", "organization", [{"id": "org-1"}, {"name": "no id"}, {"id": "org-2"}])
    if request.url.path == "/v1/organizations/org-1/adaccounts":
        accounts = [
            {"id": "acc-1", "name": "Spring", "status": "ACTIVE"},
            {"id": "acc-2", "name": "Paused", "status": "PAUSED"},
            {"id": "acc-3", "status": "ACTIVE"},
        ]
        return _wrapped("adaccounts", "adaccount", accounts)
    return _wrapped("adaccounts", "adaccount", [{"id": "acc-4", "name": "Autumn", "status": "ACTIVE"}])


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(_organizations)

        assert connection.api.client is connection.client


class TestAdAccounts:
    async def test_lists_the_active_ad_accounts_of_every_organization(self):
        urls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            return _organizations(request)

        assert await _connection(handler).ad_accounts() == [
            {"id": "acc-1", "name": "Spring"},
            {"id": "acc-3", "name": "acc-3"},
            {"id": "acc-4", "name": "Autumn"},
        ]
        assert urls == [
            ORGANIZATIONS,
            "https://adsapi.snapchat.com/v1/organizations/org-1/adaccounts",
            "https://adsapi.snapchat.com/v1/organizations/org-2/adaccounts",
        ]

    async def test_every_page_of_organizations_is_read(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if str(request.url) == ORGANIZATIONS:
                response = _wrapped("organizations", "organization", [{"id": "org-1"}])
                return httpx2.Response(
                    200, json={**response.json(), "paging": {"next_link": f"{ORGANIZATIONS}?cursor=2"}}
                )
            if "cursor" in request.url.params:
                return _wrapped("organizations", "organization", [{"id": "org-2"}])
            return _organizations(request)

        accounts = await _connection(handler).ad_accounts()

        assert [account["id"] for account in accounts] == ["acc-1", "acc-3", "acc-4"]


class TestCheck:
    async def test_a_working_token_passes(self):
        assert await _connection(_organizations).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_token_reports_the_credentials(self):
        result = await _connection(lambda _: httpx2.Response(401, json={"request_status": "ERROR"})).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False, live=True, category="auth", message="The provider rejected the credentials."
        )

    async def test_a_refused_ad_account_lookup_reports_its_status(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if str(request.url) == ORGANIZATIONS:
                return _organizations(request)
            return httpx2.Response(500, json={"request_status": "ERROR"})

        result = await _connection(handler).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False, live=True, category="error", message="The provider responded with HTTP 500."
        )


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert (
            SnapchatAdsConnection(client_id="client-id", client_secret="secret", refresh_token="refresh").client.retry
            == il.HttpRetryPolicy()
        )
