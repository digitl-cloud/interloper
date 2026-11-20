"""Tests for the Google Ads API engine.

The engine posts GAQL to a customer's ``googleAds:searchStream``, through a
manager account by the ``login-customer-id`` header, and returns the rows of
every batch of the streamed JSON array as Google sends them. Google refuses a
query with an error status, or, once the stream has started, with an error
object closing the array; both raise `GoogleApiError` with Google's reason.
These tests drive it against a fake transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.google.errors import GoogleApiError
from interloper_assets.google_ads.api import GoogleAdsApi
from interloper_assets.google_ads.constants import BASE_URL

QUERY = "SELECT campaign.id FROM campaign"


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> GoogleAdsApi:
    return GoogleAdsApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _stream(*batches: list[dict[str, Any]]) -> Callable[[httpx2.Request], httpx2.Response]:
    return lambda request: httpx2.Response(200, json=[{"results": rows, "requestId": "r1"} for rows in batches])


def _refusal(code: int, status: str, message: str) -> dict[str, Any]:
    failure = {"errors": [{"errorCode": {"queryError": "BAD_FIELD_NAME"}, "message": message}]}
    return {
        "error": {
            "code": code,
            "message": "Request contains an invalid argument.",
            "status": status,
            "details": [failure],
        }
    }


class TestGet:
    async def test_reads_the_response(self):
        sent: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(f"{request.method} {request.url}")
            return httpx2.Response(200, json={"resourceNames": ["customers/111"]})

        assert await _api(handler).get("/customers:listAccessibleCustomers") == {"resourceNames": ["customers/111"]}
        assert sent == [f"GET {BASE_URL}/customers:listAccessibleCustomers"]

    async def test_a_refusal_raises_googles_reason(self):
        api = _api(lambda request: httpx2.Response(401, json=_refusal(401, "UNAUTHENTICATED", "Not an Ads user.")))

        with pytest.raises(GoogleApiError, match=r"\(HTTP 401, UNAUTHENTICATED\): Not an Ads user\.$"):
            await api.get("/customers:listAccessibleCustomers")


class TestSearchStream:
    async def test_posts_the_query_to_the_customers_stream(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _stream([{"campaign": {"id": "1"}}])(request)

        assert await _api(handler).search_stream("123", QUERY) == [{"campaign": {"id": "1"}}]
        (request,) = requests
        assert (request.method, str(request.url)) == ("POST", f"{BASE_URL}/customers/123/googleAds:searchStream")
        assert json.loads(request.content) == {"query": QUERY}
        assert "login-customer-id" not in request.headers

    async def test_a_manager_account_is_sent_as_the_login_customer_id(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _stream()(request)

        await _api(handler).search_stream("123", QUERY, login_customer_id="999")

        assert requests[0].headers["login-customer-id"] == "999"

    async def test_reads_every_batch_in_order(self):
        api = _api(_stream([{"campaign": {"id": "1"}}, {"campaign": {"id": "2"}}], [], [{"campaign": {"id": "3"}}]))

        rows = await api.search_stream("123", QUERY)

        assert rows == [{"campaign": {"id": "1"}}, {"campaign": {"id": "2"}}, {"campaign": {"id": "3"}}]

    @pytest.mark.parametrize("body", [[], [{"fieldMask": "campaign.id", "requestId": "r1"}]])
    async def test_a_stream_without_results_has_no_rows(self, body: list[dict[str, Any]]):
        assert await _api(lambda request: httpx2.Response(200, json=body)).search_stream("123", QUERY) == []

    async def test_a_refused_query_raises_googles_reason(self):
        api = _api(lambda request: httpx2.Response(400, json=_refusal(400, "INVALID_ARGUMENT", "Unrecognized field.")))

        with pytest.raises(GoogleApiError, match=r"\(HTTP 400, INVALID_ARGUMENT\): Unrecognized field\.$"):
            await api.search_stream("123", QUERY)

    async def test_an_error_closing_the_stream_raises_googles_reason(self):
        body = [{"results": [{"campaign": {"id": "1"}}]}, _refusal(500, "INTERNAL", "Internal error encountered.")]
        api = _api(lambda request: httpx2.Response(200, json=body))

        with pytest.raises(GoogleApiError, match=r"\(HTTP 500, INTERNAL\): Internal error encountered\.$"):
            await api.search_stream("123", QUERY)


class TestReport:
    async def test_queries_the_resource_over_its_one_day(self):
        queries: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            queries.append(json.loads(request.content)["query"])
            return _stream([{"campaign": {"id": "1"}}])(request)

        rows = await _api(handler).report(
            "123", dt.date(2026, 6, 14), resource="campaign", fields=["campaign.id", "metrics.clicks"]
        )

        assert rows == [{"campaign": {"id": "1"}}]
        assert queries == [
            "SELECT campaign.id, metrics.clicks FROM campaign WHERE segments.date BETWEEN '2026-06-14' AND '2026-06-14'"
        ]

    async def test_conditions_narrow_the_day(self):
        queries: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            queries.append(json.loads(request.content)["query"])
            return _stream()(request)

        await _api(handler).report(
            "123",
            dt.date(2026, 6, 14),
            resource="campaign",
            fields=["campaign.id"],
            conditions=["campaign.status != 'REMOVED'", "metrics.impressions > 0"],
        )

        (query,) = queries
        assert query == (
            "SELECT campaign.id FROM campaign WHERE segments.date BETWEEN '2026-06-14' AND '2026-06-14' "
            "AND campaign.status != 'REMOVED' AND metrics.impressions > 0"
        )

    async def test_a_manager_account_is_sent_as_the_login_customer_id(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _stream()(request)

        await _api(handler).report(
            "123", dt.date(2026, 6, 14), resource="ad_group_ad", fields=["ad_group_ad.ad.id"], login_customer_id="999"
        )

        assert requests[0].headers["login-customer-id"] == "999"
