"""Tests for the Snapchat Marketing API engine.

Snapchat wraps every listed record twice and links the next page at
``paging.next_link``; a stats report nests its rows under
``total_stats[].total_stat.breakdown_stats[<breakdown>]``; list parameters
travel comma-separated; a refusal is an HTTP error status. These tests pin
each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.snapchat_ads.api import SnapchatAdsApi
from interloper_assets.snapchat_ads.constants import BASE_URL

CAMPAIGNS = "https://adsapi.snapchat.com/v1/adaccounts/acc-1/campaigns"
STATS = "https://adsapi.snapchat.com/v1/adaccounts/acc-1/stats"


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> SnapchatAdsApi:
    return SnapchatAdsApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _list(records: list[dict[str, Any]], next_link: str | None = None) -> httpx2.Response:
    body: dict[str, Any] = {
        "request_status": "SUCCESS",
        "campaigns": [{"sub_request_status": "SUCCESS", "campaign": record} for record in records],
    }
    if next_link is not None:
        body["paging"] = {"next_link": next_link}
    return httpx2.Response(200, json=body)


def _stats(*breakdowns: list[dict[str, Any]], next_link: str | None = None) -> httpx2.Response:
    totals = [{"total_stat": {"id": "acc-1", "breakdown_stats": {"ad": rows}}} for rows in breakdowns]
    body: dict[str, Any] = {"request_status": "SUCCESS", "total_stats": totals}
    if next_link is not None:
        body["paging"] = {"next_link": next_link}
    return httpx2.Response(200, json=body)


def _refused(status: int) -> httpx2.Response:
    return httpx2.Response(status, json={"request_status": "ERROR", "debug_message": "refused"})


class TestRows:
    async def test_unwraps_every_record_of_a_list(self):
        urls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            return _list([{"id": "c-1"}, {"id": "c-2"}], next_link=None)

        records = await _api(handler).rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign")

        assert records == [{"id": "c-1"}, {"id": "c-2"}]
        assert urls == [CAMPAIGNS]

    async def test_list_parameters_travel_comma_separated(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _list([])

        await _api(handler).rows(
            "/adaccounts/acc-1/campaigns", "campaigns", "campaign", fields=["id", "name"], limit=50
        )

        assert requests[0].url.params.multi_items() == [("fields", "id,name"), ("limit", "50")]

    async def test_every_page_is_read_by_its_next_link(self):
        urls: list[str] = []
        next_link = f"{CAMPAIGNS}?cursor=page-2"

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            if "cursor" in request.url.params:
                return _list([{"id": "c-2"}])
            return _list([{"id": "c-1"}], next_link)

        records = await _api(handler).rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign")

        assert records == [{"id": "c-1"}, {"id": "c-2"}]
        assert urls == [CAMPAIGNS, next_link]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "cursor" in request.url.params:
                return _refused(429)
            return _list([{"id": "c-1"}], f"{CAMPAIGNS}?cursor=page-2")

        with pytest.raises(httpx2.HTTPStatusError, match="429"):
            await _api(handler).rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign")

    async def test_an_http_error_raises(self):
        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await _api(lambda _: _refused(401)).rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign")

    async def test_an_empty_list_returns_no_records(self):
        urls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            return httpx2.Response(200, json={"request_status": "SUCCESS", "paging": {}})

        assert await _api(handler).rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign") == []
        assert len(urls) == 1

    async def test_a_response_without_paging_is_the_only_page(self):
        urls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            # a second request would mean the walk never ends; refuse it so the test fails rather than hangs
            if len(urls) > 1:
                return _refused(500)
            return _list([{"id": "c-1"}])

        assert await _api(handler).rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign") == [{"id": "c-1"}]
        assert len(urls) == 1

    async def test_a_wrapper_without_its_record_fails_rather_than_being_skipped(self):
        body = {"campaigns": [{"sub_request_status": "ERROR"}, {"sub_request_status": "SUCCESS", "campaign": {}}]}
        api = _api(lambda _: httpx2.Response(200, json=body))

        with pytest.raises(KeyError, match="campaign"):
            await api.rows("/adaccounts/acc-1/campaigns", "campaigns", "campaign")


class TestReport:
    async def test_totals_one_day_of_a_breakdown(self):
        requests: list[httpx2.Request] = []
        row = {"id": "ad-1", "type": "AD", "stats": {"impressions": 10}}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _stats([row])

        rows = await _api(handler).report(
            "/adaccounts/acc-1/stats",
            dt.date(2026, 10, 31),
            breakdown="ad",
            fields=["impressions", "spend"],
            report_dimension=["country"],
        )

        assert rows == [row]
        (request,) = requests
        assert str(request.url.copy_with(query=None)) == STATS
        assert request.url.params.multi_items() == [
            ("granularity", "TOTAL"),
            ("start_time", "2026-10-31"),
            ("end_time", "2026-11-01"),
            ("breakdown", "ad"),
            ("fields", "impressions,spend"),
            ("report_dimension", "country"),
        ]

    async def test_every_total_of_every_page_is_read(self):
        urls: list[str] = []
        next_link = f"{STATS}?cursor=page-2"

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            if "cursor" in request.url.params:
                return _stats([{"id": "ad-3"}])
            return _stats([{"id": "ad-1"}], [{"id": "ad-2"}], next_link=next_link)

        rows = await _api(handler).report("/adaccounts/acc-1/stats", dt.date(2026, 10, 1), breakdown="ad")

        assert rows == [{"id": "ad-1"}, {"id": "ad-2"}, {"id": "ad-3"}]
        assert len(urls) == 2 and urls[1] == next_link

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "cursor" in request.url.params:
                return _refused(400)
            return _stats([{"id": "ad-1"}], next_link=f"{STATS}?cursor=page-2")

        with pytest.raises(httpx2.HTTPStatusError, match="400"):
            await _api(handler).report("/adaccounts/acc-1/stats", dt.date(2026, 10, 1), breakdown="ad")

    async def test_a_day_without_stats_returns_no_rows(self):
        urls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            return httpx2.Response(200, json={"request_status": "SUCCESS"})

        assert await _api(handler).report("/adaccounts/acc-1/stats", dt.date(2026, 10, 1), breakdown="ad") == []
        assert len(urls) == 1

    @pytest.mark.parametrize("parameter", ["granularity", "start_time", "end_time"])
    async def test_a_parameter_of_the_window_cannot_be_overridden(self, parameter: str):
        api = _api(lambda _: _stats([]))

        with pytest.raises(TypeError, match=parameter):
            await api.report("/adaccounts/acc-1/stats", dt.date(2026, 10, 1), breakdown="ad", **{parameter: "DAY"})
