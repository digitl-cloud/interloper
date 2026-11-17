"""Tests for the Awin API engine.

Awin answers a request with its records as is, with no envelope and no pages,
and refuses one with an HTTP error status. A transaction list reads its window
as instants and a report as days. These tests pin each against a mock
transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.awin.api import AwinApi
from interloper_assets.awin.constants import BASE_URL

DAY = dt.date(2026, 10, 1)


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> AwinApi:
    return AwinApi(il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler)))


class TestGet:
    async def test_returns_one_response_as_awin_sends_it(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"userId": 9, "accounts": [{"accountId": 1}]})

        assert await _api(handler).get("/accounts", type="advertiser") == {"userId": 9, "accounts": [{"accountId": 1}]}
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == "https://api.awin.com/accounts?type=advertiser"

    async def test_a_refusal_raises_the_http_error(self):
        api = _api(lambda _: httpx2.Response(401, json={"error": "Unauthorized", "description": "invalid token"}))

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await api.get("/accounts", type="advertiser")


class TestRows:
    async def test_asks_a_list_for_every_second_of_the_day(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[{"id": 1}, {"id": 2}])

        rows = await _api(handler).rows("/advertisers/1/transactions/", DAY, timezone="UTC", showBasketProducts=True)

        assert rows == [{"id": 1}, {"id": 2}]
        (request,) = requests
        assert str(request.url) == (
            "https://api.awin.com/advertisers/1/transactions/"
            "?startDate=2026-10-01T00%3A00%3A00&endDate=2026-10-01T23%3A59%3A59&timezone=UTC&showBasketProducts=true"
        )

    async def test_an_empty_day_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[])

        assert await _api(handler).rows("/advertisers/1/transactions/", DAY, timezone="UTC") == []
        assert len(requests) == 1

    async def test_a_caller_cannot_widen_the_window(self):
        api = _api(lambda _: httpx2.Response(200, json=[]))

        with pytest.raises(TypeError, match="startDate"):
            await api.rows("/advertisers/1/transactions/", DAY, startDate="2026-01-01T00:00:00")

    async def test_a_refusal_raises_the_http_error(self):
        api = _api(lambda _: httpx2.Response(403, json={"error": "Forbidden"}))

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await api.rows("/advertisers/1/transactions/", DAY, timezone="UTC")


class TestReport:
    async def test_asks_a_report_for_the_day(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[{"publisherId": 7, "clicks": 3}])

        rows = await _api(handler).report("/advertisers/1/reports/publisher", DAY, timezone="UTC")

        assert rows == [{"publisherId": 7, "clicks": 3}]
        (request,) = requests
        assert str(request.url) == (
            "https://api.awin.com/advertisers/1/reports/publisher?startDate=2026-10-01&endDate=2026-10-01&timezone=UTC"
        )

    async def test_an_empty_day_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=[])

        assert await _api(handler).report("/advertisers/1/reports/publisher", DAY, timezone="UTC") == []
        assert len(requests) == 1

    async def test_a_caller_cannot_widen_the_window(self):
        api = _api(lambda _: httpx2.Response(200, json=[]))

        with pytest.raises(TypeError, match="endDate"):
            await api.report("/advertisers/1/reports/publisher", DAY, endDate="2026-12-31")

    async def test_a_refusal_raises_the_http_error(self):
        api = _api(lambda _: httpx2.Response(403, json={"error": "Forbidden"}))

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await api.report("/advertisers/1/reports/publisher", DAY, timezone="UTC")
