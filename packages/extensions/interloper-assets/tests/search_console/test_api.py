"""Tests for the Search Console API engine.

Every request is pinned at the HTTP level against a mock transport: its method,
URL and JSON body. Google refuses a request by its HTTP status; Search
Analytics pages by ``startRow`` up to its 25,000-row cap and returns each row's
dimension values as a positional ``keys`` list.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.search_console import constants
from interloper_assets.search_console.api import SearchConsoleApi

_BASE = "https://searchconsole.googleapis.com"
_QUERY = f"{_BASE}/webmasters/v3/sites/sc-domain%3Aexample.com/searchAnalytics/query"
_DAY = dt.date(2026, 9, 28)


def _api(*responses: tuple[int, dict[str, Any]]) -> tuple[SearchConsoleApi, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []
    remaining = list(responses)

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        status, body = remaining.pop(0)
        return httpx2.Response(status, json=body)

    client = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    return SearchConsoleApi(client), requests


def _sent(requests: list[httpx2.Request]) -> list[tuple[str, str, Any]]:
    return [(r.method, str(r.url), r.content and json.loads(r.content)) for r in requests]


def _row(*keys: str, clicks: float = 3.0) -> dict[str, Any]:
    return {"keys": list(keys), "clicks": clicks, "impressions": 40.0, "ctr": 0.075, "position": 4.25}


def _refusal(status: int, message: str) -> tuple[int, dict[str, Any]]:
    return status, {"error": {"code": status, "message": message, "status": "PERMISSION_DENIED"}}


class TestGet:
    async def test_returns_the_one_response_of_a_get(self):
        api, requests = _api((200, {"siteEntry": [{"siteUrl": "sc-domain:example.com"}]}))

        assert await api.get("/webmasters/v3/sites") == {"siteEntry": [{"siteUrl": "sc-domain:example.com"}]}
        assert _sent(requests) == [("GET", f"{_BASE}/webmasters/v3/sites", b"")]

    async def test_sends_the_query_parameters(self):
        api, requests = _api((200, {}))

        await api.get("/webmasters/v3/sites/sc-domain%3Aexample.com/sitemaps", sitemapIndex="https://example.com/a")

        assert str(requests[0].url) == (
            f"{_BASE}/webmasters/v3/sites/sc-domain%3Aexample.com/sitemaps?sitemapIndex=https%3A%2F%2Fexample.com%2Fa"
        )

    async def test_a_refusal_raises_its_status(self):
        api, requests = _api(_refusal(403, "User does not have sufficient permission for site"))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await api.get("/webmasters/v3/sites")

        assert raised.value.response.status_code == 403
        assert len(requests) == 1


class TestReport:
    async def test_queries_the_day_once_per_search_type(self):
        api, requests = _api(
            (200, {"rows": [_row("deu", "MOBILE")], "responseAggregationType": "byProperty"}),
            (200, {"rows": [_row("fra", "DESKTOP", clicks=1.0)], "responseAggregationType": "byProperty"}),
        )

        rows = await api.report(
            "sc-domain:example.com",
            _DAY,
            dimensions=["COUNTRY", "DEVICE"],
            search_types=["WEB", "DISCOVER"],
            aggregationType="AUTO",
            dataState="final",
        )

        assert rows == [
            {
                "country": "deu",
                "device": "MOBILE",
                "clicks": 3.0,
                "impressions": 40.0,
                "ctr": 0.075,
                "position": 4.25,
                "search_type": "web",
            },
            {
                "country": "fra",
                "device": "DESKTOP",
                "clicks": 1.0,
                "impressions": 40.0,
                "ctr": 0.075,
                "position": 4.25,
                "search_type": "discover",
            },
        ]
        assert _sent(requests) == [
            (
                "POST",
                _QUERY,
                {
                    "startDate": "2026-09-28",
                    "endDate": "2026-09-28",
                    "dimensions": ["COUNTRY", "DEVICE"],
                    "type": search_type,
                    "aggregationType": "AUTO",
                    "dataState": "final",
                    "rowLimit": 25_000,
                    "startRow": 0,
                },
            )
            for search_type in ("WEB", "DISCOVER")
        ]

    async def test_a_url_property_is_one_path_segment(self):
        api, requests = _api((200, {}))

        await api.report("https://example.com/shop/", _DAY, dimensions=["QUERY"], search_types=["WEB"])

        assert str(requests[0].url) == (
            f"{_BASE}/webmasters/v3/sites/https%3A%2F%2Fexample.com%2Fshop%2F/searchAnalytics/query"
        )

    async def test_pages_by_start_row_until_a_page_is_short(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        api, requests = _api((200, {"rows": full_page}), (200, {"rows": [_row("last")]}))

        rows = await api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB"])

        assert len(rows) == constants.ROW_LIMIT + 1
        assert rows[-1]["query"] == "last"
        assert [body["startRow"] for _, _, body in _sent(requests)] == [0, 25_000]

    async def test_a_full_last_page_is_followed_by_an_empty_one(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        api, requests = _api((200, {"rows": full_page}), (200, {}))

        rows = await api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB"])

        assert len(rows) == constants.ROW_LIMIT
        assert [body["startRow"] for _, _, body in _sent(requests)] == [0, 25_000]

    async def test_nothing_to_report_reads_one_page_per_search_type(self):
        api, requests = _api((200, {}), (200, {"rows": []}))

        rows = await api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB", "IMAGE"])

        assert rows == []
        assert [body["type"] for _, _, body in _sent(requests)] == ["WEB", "IMAGE"]

    async def test_a_row_without_dimensions_carries_its_metrics(self):
        api, _ = _api((200, {"rows": [{"clicks": 2.0, "impressions": 9.0, "ctr": 0.22, "position": 1.5}]}))

        assert await api.report("sc-domain:example.com", _DAY, dimensions=[], search_types=["NEWS"]) == [
            {"clicks": 2.0, "impressions": 9.0, "ctr": 0.22, "position": 1.5, "search_type": "news"}
        ]

    async def test_a_refusal_on_a_later_page_raises(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        api, _ = _api((200, {"rows": full_page}), _refusal(429, "Quota exceeded"))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB"])

        assert raised.value.response.status_code == 429
