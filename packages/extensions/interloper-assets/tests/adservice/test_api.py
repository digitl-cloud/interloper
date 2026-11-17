"""Tests for the Adservice API engine.

Adservice answers a report for its whole window at once, without pages,
wrapped in an envelope whose ``data`` holds the report, and refuses a request
with an HTTP error status. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.adservice.api import AdserviceApi

BASE_URL = "https://api.adservice.com/v2/client"
DAY = dt.date(2026, 10, 1)


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> AdserviceApi:
    return AdserviceApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _recorded(data: Any) -> tuple[AdserviceApi, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json={"success": True, "data": data})

    return _api(handler), requests


class TestReport:
    async def test_asks_for_one_day_with_the_reports_own_parameters_in_order(self):
        api, requests = _recorded([{"city": "Hamburg"}])

        rows = await api.report("/statistics/devicedetails", DAY, sales_amount=1, group_by="city")

        assert rows == [{"city": "Hamburg"}]
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == (
            f"{BASE_URL}/statistics/devicedetails?from_date=2026-10-01&to_date=2026-10-01&sales_amount=1&group_by=city"
        )

    async def test_returns_the_data_as_adservice_sends_it(self):
        statistics = {"rows": [{"stamp": "2026-10-01", "camp_id": 7}]}
        api, _ = _recorded(statistics)

        assert await api.report("/statistics", DAY) == statistics

    async def test_nothing_to_report_is_an_empty_list(self):
        api, _ = _recorded([])

        assert await api.report("/conversions", DAY) == []

    async def test_a_parameter_left_out_is_not_sent(self):
        api, requests = _recorded([])

        await api.report("/conversions", DAY)

        assert str(requests[0].url) == f"{BASE_URL}/conversions?from_date=2026-10-01&to_date=2026-10-01"

    async def test_a_refusal_raises_its_http_status(self):
        api = _api(lambda _: httpx2.Response(401, json={"success": False, "message": "Invalid API key"}))

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await api.report("/conversions", DAY)

    async def test_a_redirect_raises_without_being_followed(self):
        # api.adservice.com redirects to another host today, so every report ends here until the host is fixed
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(301, headers={"Location": "https://api.example.com/v2/client/conversions"})

        with pytest.raises(httpx2.HTTPStatusError, match="301"):
            await _api(handler).report("/conversions", DAY)
        assert len(requests) == 1

    async def test_the_window_is_the_engines_own(self):
        api, _ = _recorded([])

        with pytest.raises(TypeError):
            await api.report("/conversions", DAY, from_date="2026-09-01")
