"""Tests for the TikTok Business API engine.

TikTok answers a refused request with HTTP 200 and a non-zero ``code``, pages
its lists by ``page`` up to ``data.page_info.total_page``, and takes list and
object parameters as JSON strings. These tests pin each against a mock
transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.tiktok_ads.api import TiktokAdsApi, TiktokAdsApiError
from interloper_assets.tiktok_ads.constants import BASE_URL


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> TiktokAdsApi:
    return TiktokAdsApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _ok(data: dict[str, Any]) -> httpx2.Response:
    return httpx2.Response(200, json={"code": 0, "message": "OK", "data": data})


def _refused(code: int, message: str) -> httpx2.Response:
    return httpx2.Response(200, json={"code": code, "message": message, "data": {}})


class TestGet:
    async def test_returns_the_data_of_one_response_without_paging(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _ok({"list": [{"advertiser_id": "1"}]})

        assert await _api(handler).get("/advertiser/info/") == {"list": [{"advertiser_id": "1"}]}
        assert "page" not in requests[0].url.params
        assert "page_size" not in requests[0].url.params

    async def test_a_refusal_raises_tiktoks_code_and_message(self):
        api = _api(lambda _: _refused(40001, "The app_id is inconsistent"))

        with pytest.raises(TiktokAdsApiError, match="TikTok API error 40001: The app_id is inconsistent"):
            await api.get("/oauth2/advertiser/get/")

    async def test_an_http_error_raises_before_the_envelope_is_read(self):
        api = _api(lambda _: httpx2.Response(401, text="<html>denied</html>"))

        with pytest.raises(httpx2.HTTPStatusError):
            await api.get("/oauth2/advertiser/get/")

    async def test_list_and_object_parameters_are_sent_as_json(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _ok({"list": []})

        await _api(handler).get(
            "/advertiser/info/",
            advertiser_ids=["123"],
            filtering={"primary_status": "STATUS_ALL"},
            service_type="AUCTION",
        )

        params = requests[0].url.params
        assert json.loads(params["advertiser_ids"]) == ["123"]
        assert json.loads(params["filtering"]) == {"primary_status": "STATUS_ALL"}
        assert params["service_type"] == "AUCTION"


class TestRows:
    async def test_every_page_is_read(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            page = request.url.params["page"]
            return _ok({"list": [{"id": page}], "page_info": {"total_page": 2}})

        assert await _api(handler).rows("/campaign/get/", advertiser_id="123") == [{"id": "1"}, {"id": "2"}]
        assert {request.url.params["page_size"] for request in requests} == {"200"}

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["page"] == "2":
                return _refused(40100, "Too many requests")
            return _ok({"list": [{"id": "1"}], "page_info": {"total_page": 2}})

        with pytest.raises(TiktokAdsApiError, match="TikTok API error 40100"):
            await _api(handler).rows("/campaign/get/", advertiser_id="123")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _ok({"list": [], "page_info": {"total_page": 0}})

        assert await _api(handler).rows("/campaign/get/", advertiser_id="123") == []
        assert len(requests) == 1

    async def test_an_endpoint_without_page_info_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page ignores ``page`` and answers the same list; end a runaway walk
            if len(requests) > 3:
                return _refused(1, "runaway walk")
            return _ok({"list": [{"advertiser_id": "1"}]})

        assert await _api(handler).rows("/advertiser/info/", advertiser_ids=["1"]) == [{"advertiser_id": "1"}]
        assert len(requests) == 1


class TestReport:
    async def test_asks_the_integrated_report_for_one_day(self):
        requests: list[httpx2.Request] = []
        row = {"dimensions": {"campaign_id": "1"}, "metrics": {"spend": "2"}}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _ok({"list": [row], "page_info": {"total_page": 1}})

        rows = await _api(handler).report(
            dt.date(2026, 10, 1),
            advertiser_id="123",
            service_type="AUCTION",
            data_level="AUCTION_CAMPAIGN",
            report_type="BASIC",
            dimensions=["campaign_id", "stat_time_day"],
            metrics=["spend"],
        )

        assert rows == [row]
        (request,) = requests
        params = request.url.params
        assert request.url.path.endswith("/report/integrated/get/")
        assert (params["advertiser_id"], params["service_type"], params["data_level"], params["report_type"]) == (
            "123",
            "AUCTION",
            "AUCTION_CAMPAIGN",
            "BASIC",
        )
        assert json.loads(params["dimensions"]) == ["campaign_id", "stat_time_day"]
        assert json.loads(params["metrics"]) == ["spend"]
        assert params["start_date"] == params["end_date"] == "2026-10-01"
