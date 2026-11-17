"""Tests for the Adservice source.

Each asset reads one day of an Adservice report: one GET whose query names the
day as ``from_date`` and ``to_date``. These tests pin the request each asset
sends, the rows it returns, and the ``date`` it stamps on reports whose rows
carry no day.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import httpx2
import interloper as il

from interloper_assets.adservice.connection import AdserviceConnection
from interloper_assets.adservice.connector.source import Adservice

DAY = dt.date(2026, 10, 1)
BASE_URL = "https://api.adservice.com/v2/client"


class TestRequests:
    """Each asset sends one GET for its day, with no body.

    The client is the test's own; ``tests/adservice/test_connection.py`` pins the connection's credentials.
    """

    def _run(self, key: str, response: httpx2.Response) -> tuple[list[dict[str, Any]], list[httpx2.Request]]:
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return response

        connection = AdserviceConnection(id="adservice", api_key="key")
        connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
        source = Adservice(id="src-1", connection=connection)
        asset = next(a for a in source.assets if type(a).key == key)
        context = il.ExecutionContext(asset, il.TimePartition(value=DAY).window)
        return asyncio.run(asset.data(context=context)), requests

    def _sent(self, requests: list[httpx2.Request]) -> str:
        (request,) = requests
        assert request.method == "GET"
        assert request.content == b""
        return str(request.url)

    def test_campaigns_stats_group_the_statistics_by_day_and_campaign_with_sales_amounts(self):
        row = {"stamp": "2026-10-01", "camp_id": 7, "clicks": 3}

        rows, requests = self._run(
            "campaigns_stats", httpx2.Response(200, json={"success": True, "data": {"rows": [row]}})
        )

        assert rows == [row]
        assert self._sent(requests) == (
            f"{BASE_URL}/statistics"
            "?from_date=2026-10-01&to_date=2026-10-01&sales_amount=1&group_by=stamp%2Ccamp_id&end_group=stamp"
        )

    def test_conversions_ask_for_the_day_and_stamp_it(self):
        conversion = {"camp_id": 7, "order_id": "o-1"}

        rows, requests = self._run("conversions", httpx2.Response(200, json={"success": True, "data": [conversion]}))

        assert rows == [{**conversion, "date": DAY}]
        assert self._sent(requests) == (f"{BASE_URL}/conversions?from_date=2026-10-01&to_date=2026-10-01")

    def test_conversions_stats_by_time_of_day_ask_for_the_day_and_stamp_it(self):
        hour = {"hour": 13, "conversions": 2}

        rows, requests = self._run(
            "conversions_stats_by_time_of_day", httpx2.Response(200, json={"success": True, "data": [hour]})
        )

        assert rows == [{**hour, "date": DAY}]
        assert self._sent(requests) == (
            f"{BASE_URL}/statistics/conversions/timeofday?from_date=2026-10-01&to_date=2026-10-01"
        )

    def test_campaigns_stats_by_city_group_the_device_details_by_city(self):
        city = {"city": "Hamburg", "clicks": 4}

        rows, requests = self._run("campaigns_stats_by_city", httpx2.Response(200, json={"data": [city]}))

        assert rows == [{**city, "date": DAY}]
        assert self._sent(requests) == (
            f"{BASE_URL}/statistics/devicedetails?from_date=2026-10-01&to_date=2026-10-01&group_by=city"
        )

    def test_campaigns_stats_by_browser_group_the_device_details_by_browser(self):
        browser = {"browser": "Firefox", "clicks": 5}

        rows, requests = self._run("campaigns_stats_by_browser", httpx2.Response(200, json={"data": [browser]}))

        assert rows == [{**browser, "date": DAY}]
        assert self._sent(requests) == (
            f"{BASE_URL}/statistics/devicedetails?from_date=2026-10-01&to_date=2026-10-01&group_by=browser"
        )

    def test_campaigns_stats_by_device_type_group_the_device_details_by_device_type(self):
        device = {"device_type": "mobile", "clicks": 6}

        rows, requests = self._run("campaigns_stats_by_device_type", httpx2.Response(200, json={"data": [device]}))

        assert rows == [{**device, "date": DAY}]
        assert self._sent(requests) == (
            f"{BASE_URL}/statistics/devicedetails?from_date=2026-10-01&to_date=2026-10-01&group_by=device_type"
        )
