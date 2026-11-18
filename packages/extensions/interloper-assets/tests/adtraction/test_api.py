"""Tests for the Adtraction API engine.

Adtraction answers a list or a report with a bare JSON array and no pages,
answers a transaction list that matches nothing with an HTTP 200 message
object, and refuses a request with an HTTP error status. A list's window is
a Stockholm day as instants with their offset, a report's a day. These tests
pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.adtraction.api import AdtractionApi

BASE_URL = "https://api.adtraction.net/v2"
SUMMER_DAY = dt.date(2026, 10, 1)
WINTER_DAY = dt.date(2026, 11, 17)
CLOCKS_BACK_DAY = dt.date(2026, 10, 25)


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> AdtractionApi:
    return AdtractionApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


def _recorded(answer: Any) -> tuple[AdtractionApi, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json=answer)

    return _api(handler), requests


def _body(request: httpx2.Request) -> dict[str, Any]:
    return json.loads(request.content)


class TestGet:
    async def test_reads_one_response_as_adtraction_sends_it(self):
        account = {"programId": 52561763, "programName": "Example", "currency": "SEK", "market": "SE"}
        api, requests = _recorded(account)

        assert await api.get("/advertiser/account/") == account
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == f"{BASE_URL}/advertiser/account/"

    async def test_a_refusal_raises_its_http_status(self):
        api = _api(lambda _: httpx2.Response(401, json={"message": "Unauthorized access", "status": 401}))

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await api.get("/advertiser/account/")


class TestReport:
    async def test_posts_the_day_as_both_ends_of_the_window_then_the_reports_own_body(self):
        row = {"date": "2026-10-01", "clicks": 150}
        api, requests = _recorded([row])

        rows = await api.report("/advertiser/statistics/days/", SUMMER_DAY, transactionStatus=3)

        assert rows == [row]
        (request,) = requests
        assert request.method == "POST"
        assert str(request.url) == f"{BASE_URL}/advertiser/statistics/days/"
        assert list(_body(request).items()) == [
            ("fromDate", "2026-10-01"),
            ("toDate", "2026-10-01"),
            ("transactionStatus", 3),
        ]

    async def test_nothing_to_report_is_an_empty_list(self):
        api, _ = _recorded([])

        assert await api.report("/advertiser/statistics/ads/", SUMMER_DAY, transactionStatus=3) == []

    async def test_a_refusal_raises_its_http_status(self):
        api = _api(lambda _: httpx2.Response(403, json={"message": "Auth token not found", "status": 403}))

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await api.report("/advertiser/statistics/ads/", SUMMER_DAY, transactionStatus=3)

    async def test_the_window_is_the_engines_own(self):
        api, _ = _recorded([])

        with pytest.raises(TypeError):
            await api.report("/advertiser/statistics/ads/", SUMMER_DAY, fromDate="2026-09-01")


class TestRows:
    @pytest.mark.parametrize(
        ("day", "first", "last"),
        [
            (SUMMER_DAY, "2026-10-01T00:00:00+0200", "2026-10-01T23:59:59+0200"),
            (WINTER_DAY, "2026-11-17T00:00:00+0100", "2026-11-17T23:59:59+0100"),
            (CLOCKS_BACK_DAY, "2026-10-25T00:00:00+0200", "2026-10-25T23:59:59+0100"),
        ],
    )
    async def test_posts_the_stockholm_days_first_and_last_second_then_the_lists_own_body(self, day, first, last):
        transaction = {"orderId": "o-1", "transactionStatus": 1}
        api, requests = _recorded([transaction])

        rows = await api.rows("/advertiser/transactions/", day, transactionStatus=3)

        assert rows == [transaction]
        (request,) = requests
        assert request.method == "POST"
        assert str(request.url) == f"{BASE_URL}/advertiser/transactions/"
        assert list(_body(request).items()) == [("fromDate", first), ("toDate", last), ("transactionStatus", 3)]

    async def test_the_http_200_message_object_is_no_records(self):
        api, _ = _recorded({"message": "No transactions found.", "status": 200})

        assert await api.rows("/advertiser/transactions/", SUMMER_DAY, transactionStatus=5) == []

    async def test_any_other_message_object_raises_with_the_message(self):
        api, _ = _recorded({"message": "Something else", "status": 400})

        with pytest.raises(RuntimeError, match="Something else"):
            await api.rows("/advertiser/transactions/", SUMMER_DAY, transactionStatus=5)

    async def test_the_window_is_the_engines_own(self):
        api, _ = _recorded([])

        with pytest.raises(TypeError):
            await api.rows("/advertiser/transactions/", SUMMER_DAY, toDate="2026-10-02")
