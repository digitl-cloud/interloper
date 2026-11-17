"""Tests for the AdUp API engine.

AdUp answers a read with its JSON body, every report at one endpoint that takes
the report as a JSON body and answers its ``rows``, and refuses a request with
an HTTP error status. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.adup.api import AdupApi
from interloper_assets.adup.constants import BASE_URL

ROW = {"CampaignId": 7, "Clicks": 3, "Date": "2026-10-01"}


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> AdupApi:
    return AdupApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


class TestGet:
    async def test_returns_the_body_of_one_response(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"id": "42", "username": "advertiser"})

        assert await _api(handler).get("/advertisers/me") == {"id": "42", "username": "advertiser"}
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == "https://api.adup-tech.com/advertisers/me"

    async def test_parameters_are_sent_as_the_query(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={})

        await _api(handler).get("/advertisers/me", fields="id,label")

        assert dict(requests[0].url.params) == {"fields": "id,label"}

    async def test_a_refusal_raises_the_http_error(self):
        api = _api(lambda _: httpx2.Response(403, json={"error": "access_denied"}))

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await api.get("/advertisers/me")


class TestReport:
    async def test_asks_the_report_endpoint_for_the_window_as_json(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"rows": [ROW]})

        rows = await _api(handler).report(
            dt.date(2026, 10, 1),
            dt.date(2026, 10, 3),
            report_name="campaigns",
            report_type="CAMPAIGN_PERFORMANCE_REPORT",
            select=["CampaignId", "Clicks", "Date"],
            conditions=[{"field": "CampaignStatus", "operator": "EQUALS", "values": ["ACTIVE"]}],
        )

        assert rows == [ROW]
        (request,) = requests
        assert request.method == "POST"
        assert str(request.url) == "https://api.adup-tech.com/reports/v202101/report"
        assert list(json.loads(request.content).items()) == [
            ("report_name", "campaigns"),
            ("report_type", "CAMPAIGN_PERFORMANCE_REPORT"),
            ("select", ["CampaignId", "Clicks", "Date"]),
            ("conditions", [{"field": "CampaignStatus", "operator": "EQUALS", "values": ["ACTIVE"]}]),
            ("download_format", "JSON"),
            ("date_range_type", "CUSTOM_DATE"),
            ("date_range", {"min": "2026-10-01", "max": "2026-10-03"}),
        ]

    @pytest.mark.parametrize("key", ["download_format", "date_range_type", "date_range"])
    async def test_a_caller_cannot_override_the_format_or_the_window(self, key: str):
        api = _api(lambda _: httpx2.Response(200, json={"rows": []}))
        day = dt.date(2026, 10, 1)

        with pytest.raises(TypeError, match=key):
            await api.report(day, day, report_type="AD_PERFORMANCE_REPORT", **{key: "X"})

    async def test_an_empty_report_has_no_rows(self):
        api = _api(lambda _: httpx2.Response(200, json={"rows": []}))

        assert await api.report(dt.date(2026, 10, 1), dt.date(2026, 10, 1), report_type="AD_PERFORMANCE_REPORT") == []

    async def test_an_answer_without_rows_raises_rather_than_reading_as_empty(self):
        api = _api(lambda _: httpx2.Response(200, json={"message": "unexpected"}))

        with pytest.raises(KeyError, match="rows"):
            await api.report(dt.date(2026, 10, 1), dt.date(2026, 10, 1), report_type="AD_PERFORMANCE_REPORT")

    async def test_a_refusal_raises_the_http_error(self):
        api = _api(lambda _: httpx2.Response(400, json={"error": "invalid select"}))

        with pytest.raises(httpx2.HTTPStatusError, match="400"):
            await api.report(dt.date(2026, 10, 1), dt.date(2026, 10, 1), report_type="AD_PERFORMANCE_REPORT")
