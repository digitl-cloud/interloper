"""Tests for the Search Ads 360 Reporting API engine.

Every read is a GAQL query POSTed to the customer's ``searchAds360:search``,
paged by a ``nextPageToken`` that the next request carries in its body, and
Google refuses a request by its HTTP status. These tests pin each against a
mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.search_ads_360.api import SearchAds360Api
from interloper_assets.search_ads_360.constants import BASE_URL

SEARCH_URL = "https://searchads360.googleapis.com/v0/customers/222/searchAds360:search"


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> SearchAds360Api:
    return SearchAds360Api(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _recording(pages: list[dict[str, Any]]) -> tuple[list[httpx2.Request], Callable[[httpx2.Request], httpx2.Response]]:
    requests: list[httpx2.Request] = []
    remaining = list(pages)

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json=remaining.pop(0))

    return requests, handler


class TestSearch:
    async def test_posts_the_query_to_the_customers_search(self):
        requests, handler = _recording([{"results": [{"campaign": {"id": "1"}}]}])

        rows = await _api(handler).search("222", resource="campaign", fields=["campaign.id", "campaign.name"])

        assert rows == [{"campaign": {"id": "1"}}]
        (request,) = requests
        assert request.method == "POST"
        assert str(request.url) == SEARCH_URL
        assert json.loads(request.content) == {"query": "SELECT campaign.id, campaign.name FROM campaign"}
        assert "login-customer-id" not in request.headers

    async def test_conditions_filter_the_query(self):
        requests, handler = _recording([{"results": []}])

        await _api(handler).search(
            "222",
            resource="campaign",
            fields=["campaign.id"],
            conditions=["campaign.status = 'ENABLED'", "metrics.clicks > 0"],
        )

        assert json.loads(requests[0].content) == {
            "query": "SELECT campaign.id FROM campaign WHERE campaign.status = 'ENABLED' AND metrics.clicks > 0"
        }

    async def test_a_manager_is_sent_as_the_login_customer(self):
        requests, handler = _recording([{"results": []}])

        await _api(handler).search("222", resource="campaign", fields=["campaign.id"], login_customer_id="111")

        assert requests[0].headers["login-customer-id"] == "111"

    async def test_every_page_is_read_by_its_token(self):
        requests, handler = _recording(
            [
                {"results": [{"campaign": {"id": "1"}}], "nextPageToken": "p2"},
                {"results": [{"campaign": {"id": "2"}}], "nextPageToken": "p3"},
                {"results": [{"campaign": {"id": "3"}}]},
            ]
        )

        rows = await _api(handler).search("222", resource="campaign", fields=["campaign.id"], login_customer_id="111")

        assert rows == [{"campaign": {"id": "1"}}, {"campaign": {"id": "2"}}, {"campaign": {"id": "3"}}]
        bodies = [json.loads(request.content) for request in requests]
        query = "SELECT campaign.id FROM campaign"
        assert bodies == [{"query": query}, {"query": query, "pageToken": "p2"}, {"query": query, "pageToken": "p3"}]
        assert all(str(request.url) == SEARCH_URL for request in requests)
        assert all(request.headers["login-customer-id"] == "111" for request in requests)

    async def test_an_empty_token_ends_the_walk(self):
        requests, handler = _recording([{"results": [{"campaign": {"id": "1"}}], "nextPageToken": ""}])

        assert await _api(handler).search("222", resource="campaign", fields=["campaign.id"]) == [
            {"campaign": {"id": "1"}}
        ]
        assert len(requests) == 1

    async def test_nothing_found_has_no_rows(self):
        requests, handler = _recording([{"fieldMask": "campaign.id"}])

        assert await _api(handler).search("222", resource="campaign", fields=["campaign.id"]) == []
        assert len(requests) == 1

    async def test_a_refusal_raises_the_http_error(self):
        api = _api(lambda _: httpx2.Response(403, json={"error": {"code": 403, "status": "PERMISSION_DENIED"}}))

        with pytest.raises(httpx2.HTTPStatusError, match="403 Forbidden"):
            await api.search("222", resource="campaign", fields=["campaign.id"])

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "pageToken" in json.loads(request.content):
                return httpx2.Response(429, json={"error": {"code": 429, "status": "RESOURCE_EXHAUSTED"}})
            return httpx2.Response(200, json={"results": [{"campaign": {"id": "1"}}], "nextPageToken": "p2"})

        with pytest.raises(httpx2.HTTPStatusError, match="429 Too Many Requests"):
            await _api(handler).search("222", resource="campaign", fields=["campaign.id"])


class TestReport:
    async def test_asks_for_one_day(self):
        requests, handler = _recording([{"results": [{"segments": {"date": "2026-10-01"}}]}])

        rows = await _api(handler).report(
            "222",
            dt.date(2026, 10, 1),
            resource="campaign",
            fields=["campaign.id", "segments.date"],
            login_customer_id="111",
        )

        assert rows == [{"segments": {"date": "2026-10-01"}}]
        (request,) = requests
        assert str(request.url) == SEARCH_URL
        assert request.headers["login-customer-id"] == "111"
        assert json.loads(request.content) == {
            "query": (
                "SELECT campaign.id, segments.date FROM campaign"
                " WHERE segments.date BETWEEN '2026-10-01' AND '2026-10-01'"
            )
        }

    async def test_conditions_follow_the_day(self):
        requests, handler = _recording([{"results": []}])

        await _api(handler).report(
            "222",
            dt.date(2026, 10, 1),
            resource="campaign",
            fields=["campaign.id"],
            conditions=["campaign.status = 'ENABLED'"],
        )

        assert json.loads(requests[0].content) == {
            "query": (
                "SELECT campaign.id FROM campaign"
                " WHERE segments.date BETWEEN '2026-10-01' AND '2026-10-01' AND campaign.status = 'ENABLED'"
            )
        }
