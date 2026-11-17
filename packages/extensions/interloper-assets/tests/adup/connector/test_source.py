"""Tests for the AdUp source.

AdUp authenticates every request with an OAuth2 client-credentials token,
answers the account at ``/advertisers/me`` and every report at one endpoint,
which takes the report's fields, its days and its format as a JSON body. These
tests pin what each asset sends and what it returns, against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.adup import constants
from interloper_assets.adup.connection import AdupConnection
from interloper_assets.adup.connector.source import Adup

TOKEN = {"access_token": "access-token", "token_type": "Bearer", "expires_in": 3600}
ACCOUNT = {"id": "42", "username": "advertiser", "label": "Advertiser", "timezone": "Europe/Berlin"}
AD_ROW = {"AdName": "Ad", "CampaignId": 7, "Clicks": 3, "Cost": 1250, "Date": "2026-10-01"}


def _source(handler: Callable[[httpx2.Request], httpx2.Response]) -> Adup:
    connection = AdupConnection(id="adup", client_id="client-id", client_secret="client-secret")
    connection.__dict__["client"] = il.AsyncRESTClient(
        constants.BASE_URL, auth=connection.client.auth, transport=httpx2.MockTransport(handler)
    )
    return Adup(id="src-1", connection=connection)


async def _run(key: str, window: il.TimePartitionWindow, response: httpx2.Response) -> tuple[Any, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        if request.url.path == "/oauth2/token":
            return httpx2.Response(200, json=TOKEN)
        return response

    asset = next(a for a in _source(handler).assets if type(a).key == key)
    rows = await asset.data(context=il.ExecutionContext(asset, window))
    return rows, requests


def _day(day: dt.date) -> il.TimePartitionWindow:
    return il.TimePartitionWindow(day, day)


class TestAssets:
    def test_every_asset_is_present(self):
        assert {type(a).key for a in _source(lambda _: httpx2.Response(200)).assets} == {"account", "ads_stats"}


class TestAccount:
    async def test_reads_the_advertiser_once_and_stamps_the_snapshot_day(self):
        day = dt.date(2026, 10, 1)

        rows, requests = await _run("account", _day(day), httpx2.Response(200, json=ACCOUNT))

        assert rows == [{**ACCOUNT, "date": day}]
        token_request, request = requests
        assert token_request.url.path == "/oauth2/token"
        assert request.method == "GET"
        assert str(request.url) == "https://api.adup-tech.com/advertisers/me"
        assert request.headers["Authorization"] == "Bearer access-token"
        assert request.content == b""

    async def test_a_refused_request_raises_the_http_error(self):
        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _run("account", _day(dt.date(2026, 10, 1)), httpx2.Response(403, json={"error": "forbidden"}))


class TestAdsStats:
    async def test_asks_the_ad_performance_report_for_the_window_as_json(self):
        window = il.TimePartitionWindow(dt.date(2026, 10, 1), dt.date(2026, 10, 3))

        rows, requests = await _run("ads_stats", window, httpx2.Response(200, json={"rows": []}))

        assert rows == []
        request = requests[-1]
        assert request.method == "POST"
        assert str(request.url) == "https://api.adup-tech.com/reports/v202101/report"
        assert request.headers["Authorization"] == "Bearer access-token"
        assert request.headers["Content-Type"] == "application/json"
        assert list(json.loads(request.content).items()) == [
            ("report_name", "AD_PERFORMANCE_REPORT"),
            ("report_type", "AD_PERFORMANCE_REPORT"),
            ("select", constants.FIELDS["AD_PERFORMANCE_REPORT"]),
            ("conditions", []),
            ("download_format", "JSON"),
            ("date_range_type", "CUSTOM_DATE"),
            ("date_range", {"min": "2026-10-01", "max": "2026-10-03"}),
        ]

    async def test_returns_the_report_rows_as_adup_sends_them(self):
        response = httpx2.Response(200, json={"rows": [AD_ROW, {**AD_ROW, "Date": "2026-10-02"}]})

        rows, _ = await _run("ads_stats", _day(dt.date(2026, 10, 1)), response)

        assert rows == [AD_ROW, {**AD_ROW, "Date": "2026-10-02"}]

    async def test_a_refused_report_raises_the_http_error(self):
        with pytest.raises(httpx2.HTTPStatusError, match="400"):
            await _run("ads_stats", _day(dt.date(2026, 10, 1)), httpx2.Response(400, json={"error": "bad request"}))
