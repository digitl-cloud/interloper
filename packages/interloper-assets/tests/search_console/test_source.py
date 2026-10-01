"""Regression tests for the SearchConsole source.

Search Analytics rows carry their dimension values as a positional ``keys``
list aligned to the requested dimensions, plus ``clicks``, ``impressions``,
``ctr`` and ``position``. The source's shared query method lays each key onto a column
named after its dimension, stamps the request-only ``search_type``, and pages
with ``startRow`` past the 25,000-row response cap. Every report grouped by
``DATE`` partitions on it; ``search_appearance_stats`` is not, so it stamps
``date``.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any

import interloper as il
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_console import constants, schemas
from interloper_assets.search_console.connection import SearchConsoleConnection
from interloper_assets.search_console.source import SearchConsole

_DAY = dt.date(2026, 9, 28)

REPORT_ASSETS = {
    "page_stats": schemas.PageStats,
    "site_stats": schemas.SiteStats,
    "site_stats_by_country_device": schemas.SiteStatsByCountryDevice,
    "site_stats_by_country_page": schemas.SiteStatsByCountryPage,
    "search_appearance_stats": schemas.SearchAppearanceStats,
}


class FakeSearchAnalytics:
    """Serve canned Search Analytics pages and record every request body."""

    def __init__(self, pages: dict[str, list[list[dict[str, Any]]]]):
        """Hold the pages to serve, keyed by search type, in startRow order."""
        self.pages = pages
        self.requests: list[dict[str, Any]] = []

    def searchanalytics(self) -> FakeSearchAnalytics:
        return self

    def query(self, siteUrl: str, body: dict[str, Any]) -> SimpleNamespace:
        self.requests.append({"siteUrl": siteUrl, **body})
        pages = self.pages.get(body["type"], [])
        index = body["startRow"] // constants.ROW_LIMIT
        rows = pages[index] if index < len(pages) else []
        return SimpleNamespace(execute=lambda: {"rows": rows, "responseAggregationType": "byProperty"} if rows else {})


def _row(*keys: str, clicks: float = 3.0) -> dict[str, Any]:
    return {"keys": list(keys), "clicks": clicks, "impressions": 40.0, "ctr": 0.075, "position": 4.25}


def _connection(client: Any) -> SearchConsoleConnection:
    connection = SearchConsoleConnection(service_account_key="{}")
    connection.__dict__["client"] = lambda: client
    return connection


def _source(client: Any = None, site_url: str = "sc-domain:example.com") -> Any:
    if client is None:
        return SearchConsole(id="src-1", site_url=site_url)
    return SearchConsole(id="src-1", site_url=site_url, connection=_connection(client))


def _asset(key: str, source: Any = None) -> Any:
    return next(a for a in (source or _source()).assets if type(a).key == key)


class TestSourceAssets:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == set(REPORT_ASSETS)

    def test_all_assets_use_the_plain_normalizer(self):
        for asset in _source().assets:
            assert type(asset.normalizer) is DataFrameNormalizer, type(asset).key

    def test_all_assets_are_reports(self):
        for asset in _source().assets:
            assert asset.tags == ["Report"], type(asset).key


class TestSearchAnalyticsQuery:
    """Keys land on dimension-named columns, search types iterate, pages follow startRow."""

    def test_keys_map_onto_dimension_columns(self):
        client = FakeSearchAnalytics({"WEB": [[_row("2026-09-28", "deu", "MOBILE")]]})
        records = _source(client, site_url="sc-domain:example.com")._search_analytics(
            _DAY,
            dimensions=constants.SITE_BY_COUNTRY_DEVICE_DIMENSIONS,
            search_types=["WEB"],
        )
        assert records == [
            {
                "date": "2026-09-28",
                "country": "deu",
                "device": "MOBILE",
                "clicks": 3.0,
                "impressions": 40.0,
                "ctr": 0.075,
                "position": 4.25,
                "search_type": "web",
            }
        ]

    def test_search_appearance_dimension_column(self):
        client = FakeSearchAnalytics({"DISCOVER": [[_row("RICHCARD")]]})
        records = _source(client, site_url="sc-domain:example.com")._search_analytics(
            _DAY,
            dimensions=constants.SEARCH_APPEARANCE_DIMENSIONS,
            search_types=["DISCOVER"],
        )
        assert records[0]["search_appearance"] == "RICHCARD"
        assert records[0]["search_type"] == "discover"
        assert "keys" not in records[0]

    def test_one_request_series_per_search_type(self):
        client = FakeSearchAnalytics({"WEB": [[_row("a")]], "IMAGE": [[_row("b")]]})
        records = _source(client, site_url="https://example.com/")._search_analytics(
            _DAY,
            dimensions=["QUERY"],
            search_types=constants.SEARCH_TYPES,
            aggregation_type="BY_PROPERTY",
        )
        assert [r["search_type"] for r in records] == ["web", "image"]
        assert [r["type"] for r in client.requests] == constants.SEARCH_TYPES
        request = client.requests[0]
        assert request["siteUrl"] == "https://example.com/"
        assert request["startDate"] == request["endDate"] == "2026-09-28"
        assert request["aggregationType"] == "BY_PROPERTY"
        assert request["dataState"] == "final"
        assert request["rowLimit"] == constants.ROW_LIMIT

    def test_pages_past_the_row_limit(self):
        full_page = [_row(f"q{i}") for i in range(constants.ROW_LIMIT)]
        client = FakeSearchAnalytics({"WEB": [full_page, [_row("last")]]})
        records = _source(client, site_url="sc-domain:example.com")._search_analytics(
            _DAY,
            dimensions=["QUERY"],
            search_types=["WEB"],
        )
        assert len(records) == constants.ROW_LIMIT + 1
        assert records[-1]["query"] == "last"
        assert [r["startRow"] for r in client.requests] == [0, constants.ROW_LIMIT]

    def test_empty_response_yields_no_rows(self):
        client = FakeSearchAnalytics({})
        records = _source(client, site_url="sc-domain:example.com")._search_analytics(
            _DAY,
            dimensions=["QUERY"],
            search_types=["WEB"],
        )
        assert records == []
        assert len(client.requests) == 1


class TestAssetRequests:
    """Each asset sends the dimensions, search types and aggregation of its old report."""

    def _requests(self, key: str) -> list[dict[str, Any]]:
        client = FakeSearchAnalytics({})
        source = _source(client)
        _asset(key, source).run(il.TimePartition(_DAY))
        return client.requests

    def test_page_stats(self):
        requests = self._requests("page_stats")
        assert [r["type"] for r in requests] == constants.SEARCH_TYPES
        assert requests[0]["dimensions"] == constants.PAGE_DIMENSIONS
        assert requests[0]["aggregationType"] == "BY_PAGE"

    def test_site_stats(self):
        requests = self._requests("site_stats")
        assert [r["type"] for r in requests] == constants.SEARCH_TYPES
        assert requests[0]["dimensions"] == constants.SITE_DIMENSIONS
        assert requests[0]["aggregationType"] == "BY_PROPERTY"

    def test_site_stats_by_country_device(self):
        requests = self._requests("site_stats_by_country_device")
        assert [r["type"] for r in requests] == constants.SEARCH_TYPES
        assert requests[0]["dimensions"] == constants.SITE_BY_COUNTRY_DEVICE_DIMENSIONS
        assert requests[0]["aggregationType"] == "AUTO"

    def test_site_stats_by_country_page(self):
        requests = self._requests("site_stats_by_country_page")
        assert [r["type"] for r in requests] == constants.ALL_SEARCH_TYPES
        assert requests[0]["dimensions"] == constants.SITE_BY_COUNTRY_PAGE_DIMENSIONS

    def test_search_appearance_stats_stamps_the_partition_day(self):
        client = FakeSearchAnalytics({"WEB": [[_row("VIDEO")]]})
        source = _source(client)
        rows = _asset("search_appearance_stats", source).run(il.TimePartition(_DAY)).to_dict("records")
        assert [r["type"] for r in client.requests] == constants.ALL_SEARCH_TYPES
        assert client.requests[0]["dimensions"] == constants.SEARCH_APPEARANCE_DIMENSIONS
        assert rows[0]["date"] == _DAY
        assert rows[0]["search_appearance"] == "VIDEO"


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
        client = FakeSearchAnalytics({"WEB": [[_row("2026-09-28", "https://example.com/a", "shoes", "deu", "MOBILE")]]})
        records = _source(client, site_url="sc-domain:example.com")._search_analytics(
            _DAY,
            dimensions=constants.PAGE_DIMENSIONS,
            search_types=["WEB"],
        )
        normalized = self._child("page_stats").normalizer.normalize(records)
        reconciled = Representation.of(normalized).reconcile(schemas.PageStats)
        Representation.of(reconciled).reconcile(schemas.PageStats, strict=True)
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
        reconciled = Representation.of(normalized).reconcile(schemas.SearchAppearanceStats)
        Representation.of(reconciled).reconcile(schemas.SearchAppearanceStats, strict=True)
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
