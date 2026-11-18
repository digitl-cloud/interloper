"""Regression tests for the SearchConsole source.

Every asset is one Search Analytics report through the connection's engine,
which lays each row's positional ``keys`` onto columns named after the
requested dimensions, stamps the request-only ``search_type`` and pages with
``startRow`` (pinned in ``test_api.py``). These tests pin what each asset sends
and returns. Every report grouped by ``DATE`` partitions on it;
``search_appearance_stats`` is not, so it stamps ``date``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any
from urllib.parse import unquote

import httpx2
import interloper as il
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_console import constants
from interloper_assets.search_console.connection import SearchConsoleConnection
from interloper_assets.search_console.connector import schemas
from interloper_assets.search_console.connector.source import SearchConsole

_DAY = dt.date(2026, 9, 28)

REPORT_ASSETS = {
    "page_stats": schemas.PageStats,
    "site_stats": schemas.SiteStats,
    "site_stats_by_country_device": schemas.SiteStatsByCountryDevice,
    "site_stats_by_country_page": schemas.SiteStatsByCountryPage,
    "search_appearance_stats": schemas.SearchAppearanceStats,
}


class FakeSearchAnalytics:
    """Serve canned Search Analytics pages over HTTP and record every query."""

    def __init__(self, pages: dict[str, list[list[dict[str, Any]]]], refusal: int | None = None):
        """Hold the pages to serve, keyed by search type, in startRow order, or the status to refuse with."""
        self.pages = pages
        self.refusal = refusal
        self.requests: list[dict[str, Any]] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        site_url = unquote(request.url.raw_path.decode().split("/")[4])
        body = json.loads(request.content)
        self.requests.append({"siteUrl": site_url, **body})
        if self.refusal is not None:
            return httpx2.Response(self.refusal, json={"error": {"code": self.refusal, "message": "No access"}})
        pages = self.pages.get(body["type"], [])
        index = body["startRow"] // constants.ROW_LIMIT
        rows = pages[index] if index < len(pages) else []
        return httpx2.Response(200, json={"rows": rows, "responseAggregationType": "byProperty"} if rows else {})


def _row(*keys: str, clicks: float = 3.0) -> dict[str, Any]:
    return {"keys": list(keys), "clicks": clicks, "impressions": 40.0, "ctr": 0.075, "position": 4.25}


def _connection(server: FakeSearchAnalytics) -> SearchConsoleConnection:
    connection = SearchConsoleConnection(service_account_key="{}")
    connection.__dict__["client"] = il.AsyncRESTClient(
        constants.BASE_URL, transport=httpx2.MockTransport(server), retry=il.RetryPolicy(max_attempts=1)
    )
    return connection


def _source(server: FakeSearchAnalytics | None = None, site_url: str = "sc-domain:example.com") -> Any:
    if server is None:
        return SearchConsole(id="src-1", site_url=site_url, connection=SearchConsoleConnection(service_account_key="x"))
    return SearchConsole(id="src-1", site_url=site_url, connection=_connection(server))


def _asset(key: str, source: Any = None) -> Any:
    return next(a for a in (source or _source()).assets if type(a).key == key)


def _data(key: str, server: FakeSearchAnalytics) -> list[dict[str, Any]]:
    asset = _asset(key, _source(server))
    return asyncio.run(asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=_DAY).window)))


class TestSourceAssets:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == set(REPORT_ASSETS)

    def test_all_assets_use_the_plain_normalizer(self):
        for asset in _source().assets:
            assert type(asset.normalizer) is DataFrameNormalizer, type(asset).key

    def test_all_assets_are_reports(self):
        for asset in _source().assets:
            assert asset.tags == ["Report"], type(asset).key


class TestAssetQueries:
    """Each asset sends exactly its old queries and returns exactly its old rows."""

    @pytest.mark.parametrize(
        ("key", "dimensions", "search_types", "aggregation_type", "stamped"),
        [
            ("page_stats", constants.PAGE_DIMENSIONS, constants.SEARCH_TYPES, "BY_PAGE", {}),
            ("site_stats", constants.SITE_DIMENSIONS, constants.SEARCH_TYPES, "BY_PROPERTY", {}),
            (
                "site_stats_by_country_device",
                constants.SITE_BY_COUNTRY_DEVICE_DIMENSIONS,
                constants.SEARCH_TYPES,
                "AUTO",
                {},
            ),
            (
                "site_stats_by_country_page",
                constants.SITE_BY_COUNTRY_PAGE_DIMENSIONS,
                constants.ALL_SEARCH_TYPES,
                "AUTO",
                {},
            ),
            (
                "search_appearance_stats",
                constants.SEARCH_APPEARANCE_DIMENSIONS,
                constants.ALL_SEARCH_TYPES,
                "AUTO",
                {"date": _DAY},
            ),
        ],
    )
    def test_an_asset_queries_its_day_once_per_search_type(
        self,
        key: str,
        dimensions: list[str],
        search_types: list[str],
        aggregation_type: str,
        stamped: dict[str, Any],
    ):
        pages = {search_type: [[_row(*(f"{search_type}-{d}" for d in dimensions))]] for search_type in search_types}
        server = FakeSearchAnalytics(pages)

        rows = _data(key, server)

        assert server.requests == [
            {
                "siteUrl": "sc-domain:example.com",
                "startDate": "2026-09-28",
                "endDate": "2026-09-28",
                "dimensions": dimensions,
                "type": search_type,
                "aggregationType": aggregation_type,
                "dataState": "final",
                "rowLimit": 25_000,
                "startRow": 0,
            }
            for search_type in search_types
        ]
        assert rows == [
            {
                **{dimension.lower(): f"{search_type}-{dimension}" for dimension in dimensions},
                "clicks": 3.0,
                "impressions": 40.0,
                "ctr": 0.075,
                "position": 4.25,
                "search_type": search_type.lower(),
                **stamped,
            }
            for search_type in search_types
        ]

    def test_an_asset_pages_past_the_row_limit(self):
        full_page = [_row("2026-09-28", "deu", f"device-{i}") for i in range(constants.ROW_LIMIT)]
        server = FakeSearchAnalytics({"WEB": [full_page, [_row("2026-09-28", "fra", "MOBILE")]]})

        rows = _data("site_stats_by_country_device", server)

        assert len(rows) == constants.ROW_LIMIT + 1
        assert rows[-1]["country"] == "fra"
        assert [(r["type"], r["startRow"]) for r in server.requests] == [
            ("WEB", 0),
            ("WEB", 25_000),
            ("IMAGE", 0),
            ("VIDEO", 0),
            ("NEWS", 0),
        ]

    def test_an_asset_with_nothing_to_report_returns_no_rows(self):
        server = FakeSearchAnalytics({})

        assert _data("search_appearance_stats", server) == []
        assert len(server.requests) == len(constants.ALL_SEARCH_TYPES)

    def test_a_refusal_raises_out_of_the_asset_with_its_status(self):
        server = FakeSearchAnalytics({}, refusal=403)

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            _data("page_stats", server)

        assert raised.value.response.status_code == 403


class TestSpecRoundtripAndReconcile:
    """The normalizer survives the host-to-child round-trip and rows reconcile with correct types."""

    def _child(self, key: str) -> Any:
        source = _source()
        asset = _asset(key, source)
        spec_json = DAG(source).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_every_asset_round_trips(self):
        for key in REPORT_ASSETS:
            assert type(self._child(key).normalizer) is DataFrameNormalizer, key

    def test_dated_report_row_reconciles(self):
        server = FakeSearchAnalytics({"WEB": [[_row("2026-09-28", "https://example.com/a", "shoes", "deu", "MOBILE")]]})
        records = _data("page_stats", server)
        normalized = self._child("page_stats").normalizer.normalize(records)
        reconciled = TableData.from_data(normalized).reconcile(schemas.PageStats)
        TableData.from_data(reconciled).reconcile(schemas.PageStats, strict=True)
        record = reconciled.to_dict("records")[0]
        assert record["date"] == _DAY
        assert record["page"] == "https://example.com/a"
        assert record["query"] == "shoes"
        assert record["clicks"] == 3 and isinstance(record["clicks"], int)
        assert record["impressions"] == 40
        assert record["ctr"] == 0.075
        assert record["position"] == 4.25
        assert record["search_type"] == "web"

    def test_search_appearance_row_reconciles(self):
        rows = [{"search_appearance": "VIDEO", "clicks": 1.0, "impressions": 9.0, "ctr": 0.111, "position": 2.5}]
        stamped = [{**row, "search_type": "web", "date": _DAY} for row in rows]
        normalized = self._child("search_appearance_stats").normalizer.normalize(stamped)
        reconciled = TableData.from_data(normalized).reconcile(schemas.SearchAppearanceStats)
        TableData.from_data(reconciled).reconcile(schemas.SearchAppearanceStats, strict=True)
        record = reconciled.to_dict("records")[0]
        assert record["date"] == _DAY
        assert record["impressions"] == 9


class TestPartitionColumns:
    """Reports grouped by DATE partition on it; search_appearance_stats partitions on a stamped date."""

    def test_every_asset_partitions_on_date(self):
        for key, schema in REPORT_ASSETS.items():
            asset = _asset(key)
            assert asset.partitioning is not None and asset.partitioning.column == "date", key
            assert schema.model_fields["date"].annotation == (dt.date | None), key

    def test_dated_reports_request_the_date_dimension(self):
        for dimensions in (
            constants.PAGE_DIMENSIONS,
            constants.SITE_DIMENSIONS,
            constants.SITE_BY_COUNTRY_DEVICE_DIMENSIONS,
            constants.SITE_BY_COUNTRY_PAGE_DIMENSIONS,
        ):
            assert "DATE" in dimensions

    def test_search_appearance_does_not_request_the_date_dimension(self):
        assert "DATE" not in constants.SEARCH_APPEARANCE_DIMENSIONS
