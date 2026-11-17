"""Tests for the Search Console API engine.

The engine drives Google's discovery client, so these tests build a real one
over googleapiclient's recording HTTP mock: every request is encoded as it
would be sent. Google refuses a request with an HTTP status, which the client
raises as ``HttpError``; Search Analytics pages by ``startRow`` up to its
25,000-row cap and returns each row's dimension values as a positional
``keys`` list.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pytest
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpMockSequence

from interloper_assets.search_console import constants
from interloper_assets.search_console.api import SearchConsoleApi

_BASE = "https://searchconsole.googleapis.com"
_QUERY = f"{_BASE}/webmasters/v3/sites/sc-domain%3Aexample.com/searchAnalytics/query?alt=json"
_DAY = dt.date(2026, 9, 28)


class _Unreachable:
    """An HTTP transport whose every request times out."""

    def request(self, *args: Any, **kwargs: Any) -> Any:
        raise TimeoutError("timed out")


def _api(*responses: tuple[int, dict[str, Any]]) -> tuple[SearchConsoleApi, HttpMockSequence]:
    http = HttpMockSequence([({"status": str(status)}, json.dumps(body)) for status, body in responses])
    return SearchConsoleApi(build(constants.API_SERVICE, constants.API_VERSION, http=http)), http


def _sent(http: HttpMockSequence) -> list[tuple[str, str, Any]]:
    return [(method, uri, body and json.loads(body)) for uri, method, body, _ in http.request_sequence]


def _row(*keys: str, clicks: float = 3.0) -> dict[str, Any]:
    return {"keys": list(keys), "clicks": clicks, "impressions": 40.0, "ctr": 0.075, "position": 4.25}


def _refusal(status: int, message: str) -> tuple[int, dict[str, Any]]:
    return status, {"error": {"code": status, "message": message, "status": "PERMISSION_DENIED"}}


class TestGet:
    def test_returns_the_one_response_of_a_method(self):
        api, http = _api((200, {"siteEntry": [{"siteUrl": "sc-domain:example.com"}]}))

        assert api.get("sites.list") == {"siteEntry": [{"siteUrl": "sc-domain:example.com"}]}
        assert _sent(http) == [("GET", f"{_BASE}/webmasters/v3/sites?alt=json", None)]

    def test_sends_the_methods_parameters(self):
        api, http = _api((200, {}))

        api.get("sitemaps.list", siteUrl="sc-domain:example.com")

        assert _sent(http) == [("GET", f"{_BASE}/webmasters/v3/sites/sc-domain%3Aexample.com/sitemaps?alt=json", None)]

    def test_reaches_a_method_of_a_nested_resource(self):
        api, http = _api((200, {"inspectionResult": {}}))
        body = {"inspectionUrl": "https://example.com/a", "siteUrl": "sc-domain:example.com"}

        assert api.get("urlInspection.index.inspect", body=body) == {"inspectionResult": {}}
        assert _sent(http) == [("POST", f"{_BASE}/v1/urlInspection/index:inspect?alt=json", body)]

    def test_a_refusal_raises_googles_status_and_message(self):
        api, http = _api(_refusal(403, "User does not have sufficient permission for site"))

        with pytest.raises(HttpError, match="User does not have sufficient permission for site") as raised:
            api.get("sites.list")

        assert raised.value.status_code == 403
        assert len(http.request_sequence) == 1

    def test_a_server_error_raises_without_a_retry(self):
        api, http = _api((500, {"error": {"code": 500, "message": "Internal error"}}), (200, {}))

        with pytest.raises(HttpError) as raised:
            api.get("sites.list")

        assert raised.value.status_code == 500
        assert len(http.request_sequence) == 1

    def test_a_transport_failure_raises_as_is(self):
        api = SearchConsoleApi(build(constants.API_SERVICE, constants.API_VERSION, http=_Unreachable()))

        with pytest.raises(TimeoutError, match="timed out"):
            api.get("sites.list")


class TestReport:
    def test_queries_the_day_once_per_search_type(self):
        api, http = _api(
            (200, {"rows": [_row("deu", "MOBILE")], "responseAggregationType": "byProperty"}),
            (200, {"rows": [_row("fra", "DESKTOP", clicks=1.0)], "responseAggregationType": "byProperty"}),
        )

        rows = api.report(
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
        assert _sent(http) == [
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

    def test_pages_by_start_row_until_a_page_is_short(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        api, http = _api((200, {"rows": full_page}), (200, {"rows": [_row("last")]}))

        rows = api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB"])

        assert len(rows) == constants.ROW_LIMIT + 1
        assert rows[-1]["query"] == "last"
        assert [body["startRow"] for _, _, body in _sent(http)] == [0, 25_000]

    def test_a_full_last_page_is_followed_by_an_empty_one(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        api, http = _api((200, {"rows": full_page}), (200, {}))

        rows = api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB"])

        assert len(rows) == constants.ROW_LIMIT
        assert [body["startRow"] for _, _, body in _sent(http)] == [0, 25_000]

    def test_nothing_to_report_reads_one_page_per_search_type(self):
        api, http = _api((200, {}), (200, {"rows": []}))

        assert api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB", "IMAGE"]) == []
        assert [body["type"] for _, _, body in _sent(http)] == ["WEB", "IMAGE"]

    def test_a_row_without_dimensions_carries_its_metrics(self):
        api, _ = _api((200, {"rows": [{"clicks": 2.0, "impressions": 9.0, "ctr": 0.22, "position": 1.5}]}))

        assert api.report("sc-domain:example.com", _DAY, dimensions=[], search_types=["NEWS"]) == [
            {"clicks": 2.0, "impressions": 9.0, "ctr": 0.22, "position": 1.5, "search_type": "news"}
        ]

    def test_a_refusal_on_a_later_page_raises(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        api, _ = _api((200, {"rows": full_page}), _refusal(429, "Quota exceeded"))

        with pytest.raises(HttpError, match="Quota exceeded"):
            api.report("sc-domain:example.com", _DAY, dimensions=["QUERY"], search_types=["WEB"])
