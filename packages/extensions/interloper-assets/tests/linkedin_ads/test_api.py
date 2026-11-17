"""Tests for the LinkedIn Marketing API engine.

LinkedIn reads its query in Rest.li 2.0 syntax, which an ordinary query
encoder breaks, wraps every list in ``elements``, pages its finders either by
a ``nextPageToken`` cursor or by ``start``/``count``, and refuses a request
with an HTTP error status. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.linkedin_ads.api import LinkedinAdsApi
from interloper_assets.linkedin_ads.constants import BASE_URL

DAY = dt.date(2026, 9, 15)


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> LinkedinAdsApi:
    return LinkedinAdsApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _params(request: httpx2.Request) -> dict[str, str]:
    pairs = [part.split("=", 1) for part in request.url.query.decode().split("&") if part]
    params = dict(pairs)
    assert len(params) == len(pairs), f"a parameter is sent twice: {pairs}"
    return params


class TestGet:
    async def test_returns_the_body_of_one_response(self):
        requests: list[httpx2.Request] = []
        account = {"id": 123, "name": "Acme"}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=account)

        body = await _api(handler).get("/adAccounts/123", fields=["id", "name"])

        assert body == account
        (request,) = requests
        assert request.method == "GET"
        assert request.url.path == "/rest/adAccounts/123"
        assert request.url.query.decode() == "fields=id,name"

    async def test_a_path_without_parameters_is_sent_without_a_query(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={})

        await _api(handler).get("/adAccounts/123")

        assert str(requests[0].url) == "https://api.linkedin.com/rest/adAccounts/123"

    async def test_an_http_error_raises_its_status(self):
        api = _api(lambda _: httpx2.Response(401, json={"status": 401, "code": "REVOKED_ACCESS_TOKEN"}))

        with pytest.raises(httpx2.HTTPStatusError, match="401 Unauthorized"):
            await api.get("/adAccounts/123")


class TestRestliQuery:
    async def _sent(self, **params: Any) -> dict[str, str]:
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={})

        await _api(handler).get("/adAnalytics", **params)
        return _params(requests[0])

    async def test_lists_records_and_days_are_written_in_restli_syntax(self):
        sent = await self._sent(
            pivots=["SHARE", "CAMPAIGN"],
            timeGranularity={"value": "DAILY"},
            dateRange={"start": DAY, "end": DAY},
            search={"status": {"values": ["ACTIVE", "PAUSED"]}},
        )

        assert sent == {
            "pivots": "List(SHARE,CAMPAIGN)",
            "timeGranularity": "(value:DAILY)",
            "dateRange": "(start:(year:2026,month:9,day:15),end:(year:2026,month:9,day:15))",
            "search": "(status:(values:List(ACTIVE,PAUSED)))",
        }

    async def test_a_value_is_percent_encoded_down_to_its_reserved_characters(self):
        sent = await self._sent(accounts=["urn:li:sponsoredAccount:123"], name="a,b (c)/d")

        assert sent == {"accounts": "List(urn%3Ali%3AsponsoredAccount%3A123)", "name": "a%2Cb%20%28c%29%2Fd"}

    async def test_numbers_and_booleans_are_written_as_restli_reads_them(self):
        sent = await self._sent(count=10, test=True, archived=False)

        assert sent == {"count": "10", "test": "true", "archived": "false"}

    async def test_the_projection_is_a_bare_comma_separated_list(self):
        sent = await self._sent(q="statistics", fields=["pivotValues", "dateRange", "impressions"])

        assert sent == {"q": "statistics", "fields": "pivotValues,dateRange,impressions"}


class TestRows:
    async def test_every_page_is_read_by_its_token(self):
        requests: list[httpx2.Request] = []
        pages = {
            None: {"elements": [{"id": 1}], "metadata": {"nextPageToken": "tok/1"}},
            "tok%2F1": {"elements": [{"id": 2}], "metadata": {}},
        }

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=pages[_params(request).get("pageToken")])

        rows = await _api(handler).rows("/adAccounts/123/adCampaigns", q="search", fields=["id"])

        assert rows == [{"id": 1}, {"id": 2}]
        assert [request.url.path for request in requests] == ["/rest/adAccounts/123/adCampaigns"] * 2
        assert _params(requests[0]) == {"q": "search", "fields": "id", "pageSize": "1000"}
        assert _params(requests[1]) == {"q": "search", "fields": "id", "pageSize": "1000", "pageToken": "tok%2F1"}

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "pageToken" in _params(request):
                return httpx2.Response(403, json={"status": 403, "code": "ACCESS_DENIED"})
            return httpx2.Response(200, json={"elements": [{"id": 1}], "metadata": {"nextPageToken": "2"}})

        with pytest.raises(httpx2.HTTPStatusError, match="403 Forbidden"):
            await _api(handler).rows("/adAccounts/123/adCampaigns", q="search")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"paging": {"start": 0, "count": 1000}, "elements": []})

        assert await _api(handler).rows("/adAccounts/123/adCampaigns", q="search") == []
        assert len(requests) == 1

    @pytest.mark.parametrize("paging", [{}, {"metadata": None}, {"metadata": {}}])
    async def test_a_response_without_paging_info_is_read_once(self, paging: dict[str, Any]):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page answers the same list forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(200, json={})
            return httpx2.Response(200, json={"elements": [{"id": 1}], **paging})

        assert await _api(handler).rows("/adAccounts/123/adCampaigns", q="search") == [{"id": 1}]
        assert len(requests) == 1


class TestOffsetRows:
    async def test_every_page_is_read_until_an_empty_one(self):
        sent: list[str] = []
        pages = {"0": [{"id": 1}, {"id": 2}], "100": [{"id": 3}], "200": []}

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(f"{request.url.path}?{request.url.query.decode()}")
            return httpx2.Response(200, json={"elements": pages[request.url.params["start"]]})

        rows = await _api(handler).offset_rows("/adAccounts", q="search")

        assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert sent == [
            "/rest/adAccounts?q=search&count=100&start=0",
            "/rest/adAccounts?q=search&count=100&start=100",
            "/rest/adAccounts?q=search&count=100&start=200",
        ]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["start"] != "0":
                return httpx2.Response(403, json={"status": 403, "code": "ACCESS_DENIED"})
            return httpx2.Response(200, json={"elements": [{"id": 1}]})

        with pytest.raises(httpx2.HTTPStatusError, match="403 Forbidden"):
            await _api(handler).offset_rows("/adAccounts", q="search")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"paging": {"start": 0, "count": 100, "total": 0}, "elements": []})

        assert await _api(handler).offset_rows("/adAccounts", q="search") == []
        assert len(requests) == 1

    async def test_a_page_without_elements_fails_rather_than_ending_the_list(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["start"] == "0":
                return httpx2.Response(200, json={"elements": [{"id": 1}]})
            return httpx2.Response(200, json={"paging": {"start": 100, "count": 100}})

        with pytest.raises(KeyError, match="elements"):
            await _api(handler).offset_rows("/adAccounts", q="search")

    async def test_an_endpoint_that_ignores_the_offset_is_read_as_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that ignores ``start`` answers the same page forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(200, json={"elements": []})
            return httpx2.Response(200, json={"elements": [{"id": 1}, {"id": 2}]})

        assert await _api(handler).offset_rows("/adAccounts", q="search") == [{"id": 1}, {"id": 2}]
        assert len(requests) == 2


class TestReport:
    async def test_asks_ad_analytics_for_one_day(self):
        requests: list[httpx2.Request] = []
        row = {"pivotValues": ["urn:li:sponsoredCampaign:42"], "impressions": 3}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"paging": {"start": 0, "count": 10}, "elements": [row]})

        rows = await _api(handler).report(
            DAY,
            q="analytics",
            pivot="CAMPAIGN",
            timeGranularity={"value": "DAILY"},
            accounts=["urn:li:sponsoredAccount:123"],
            fields=["pivotValues", "impressions"],
        )

        assert rows == [row]
        (request,) = requests
        assert request.method == "GET"
        assert request.url.path == "/rest/adAnalytics"
        assert _params(request) == {
            "q": "analytics",
            "pivot": "CAMPAIGN",
            "timeGranularity": "(value:DAILY)",
            "accounts": "List(urn%3Ali%3AsponsoredAccount%3A123)",
            "fields": "pivotValues,impressions",
            "dateRange": "(start:(year:2026,month:9,day:15),end:(year:2026,month:9,day:15))",
        }

    async def test_a_day_without_rows_is_empty(self):
        assert await _api(lambda _: httpx2.Response(200, json={})).report(DAY, q="statistics") == []
