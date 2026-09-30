"""Regression tests for the Teads source.

The report job's CSV carries Teads' display-name headers ("Budget delivered - CPC",
"Line ID", ...), not the request's dimension/metric keys; the base normalizer's
casing pass lands them on the schema columns. These tests pin the asset set, the
normalizer, the spec round-trip, reconciliation of a vendor-shaped row, the
partition column, and the request/poll/download flow against a faked Teads API.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import pytest
from interloper.asset.context import ExecutionContext
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.teads import constants, schemas
from interloper_assets.teads.connection import TeadsConnection
from interloper_assets.teads.source import Teads

CSV = (
    "Day,Advertiser ID,Advertiser name,Campaign external integration code,Campaign ID,Campaign name,"
    "Creative ID,Creative name,Creative external integration code,Line ID,Line item name,"
    "Line item billable event,Line item budget,Budget spent,Video completion rate,"
    "Budget delivered - Total ad cost,Budget delivered,Clicks,Billable events,Clickthrough rate,"
    "Video starts,Budget delivered - CPC,Video completes,Budget delivered - CPM\n"
    "2026-06-14,20765,Acme,,111,Summer,222,Hero 15s,EXT-9,333,Line A,complete,5000.0,"
    "120.5,0.72,130.25,125.0,42,900,0.021,1250,2.98,900,13.4\n"
)


def _source(connection: TeadsConnection | None = None) -> Any:
    return Teads(id="src-1", advertiser_id=20765, connection=connection)


def _asset(source: Any = None) -> Any:
    return next(a for a in (source or _source()).assets if type(a).key == "creatives_stats")


class TestAssets:
    def test_asset_key_set(self):
        assert {type(a).key for a in _source().assets} == {"creatives_stats"}

    def test_asset_uses_the_source_normalizer(self):
        normalizer = _asset().normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        assert normalizer.replace_empty_strings is True

    def test_advertiser_id_discriminates_the_tables(self):
        source = _source()
        assert source.discriminator == "20765"
        assert source.asset_table(_asset(source)) == "creatives_stats__20765"


class TestSpecRoundtripAndReconcile:
    def _child(self) -> Any:
        source = _source()
        spec_json = DAG(source).mini_dag(_asset(source).id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == "creatives_stats")

    def test_csv_row_reconciles_with_typed_columns(self):
        child = self._child()
        assert isinstance(child.normalizer, DataFrameNormalizer)
        rows = list(csv.DictReader(CSV.splitlines()))
        normalized = child.normalizer.normalize(rows)
        assert set(normalized.columns) == set(schemas.CreativesStats.model_fields)
        reconciled = Representation.of(normalized).reconcile(schemas.CreativesStats)
        row = reconciled.loc[0]
        assert row["day"] == dt.date(2026, 6, 14)
        assert int(row["campaign_id"]) == 111 and int(row["line_id"]) == 333
        assert int(row["clicks"]) == 42 and int(row["video_starts"]) == 1250
        assert float(row["budget_delivered_cpc"]) == 2.98
        assert float(row["video_completion_rate"]) == 0.72
        assert row["creative_external_integration_code"] == "EXT-9"
        assert pd.isna(row["campaign_external_integration_code"])


class TestPartitionColumn:
    def test_partitions_on_the_vendor_day(self):
        asset = _asset()
        assert asset.tags == ["Report"]
        assert asset.partitioning is not None and asset.partitioning.column == "day"
        assert asset.schema is not None
        assert "day" in asset.schema.model_fields
        assert "date" not in asset.schema.model_fields


class TestReportFlow:
    """Request the job, poll its status, download the CSV the finished status points at."""

    def _run(self, monkeypatch: Any, statuses: list[dict[str, Any]], run: dict[str, Any] | None = None) -> Any:
        requests: list[httpx2.Request] = []
        remaining = list(statuses)

        def api(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.path == "/api/reports/run":
                return httpx2.Response(200, json=run or {"valid": True, "id": "r-1"})
            return httpx2.Response(200, json=remaining.pop(0))

        def storage(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, text=CSV)

        connection = TeadsConnection(api_key="secret")
        monkeypatch.setitem(
            connection.__dict__,
            "client",
            il.AsyncRESTClient(
                constants.BASE_URL, headers={"Authorization": "secret"}, transport=httpx2.MockTransport(api)
            ),
        )
        real_client = httpx2.AsyncClient
        monkeypatch.setattr(
            httpx2, "AsyncClient", lambda **kwargs: real_client(transport=httpx2.MockTransport(storage), **kwargs)
        )
        monkeypatch.setattr(constants, "REPORT_POLL_INTERVAL", 0)

        asset = _asset(_source(connection))
        context = ExecutionContext(
            asset_key=asset.key,
            partitioning=asset.partitioning,
            partition_or_window=il.TimePartition(value=dt.date(2026, 6, 14)),
        )
        return asyncio.run(asset.data(context=context)), requests

    def test_polls_until_finished_and_parses_the_csv(self, monkeypatch: Any):
        rows, requests = self._run(
            monkeypatch,
            [{"status": "running"}, {"status": "finished", "uri": "https://storage.example/r-1.csv"}],
        )
        assert len(rows) == 1 and rows[0]["Creative ID"] == "222"

        run, *polls, download = requests
        body = json.loads(run.content)
        assert body["filters"]["advertisers"] == [20765]
        assert body["filters"]["date"]["start"] == "2026-06-14T00:00:00.000+00:00"
        assert body["filters"]["date"]["end"] == "2026-06-14T23:59:59.999+00:00"
        assert body["dimensions"] == constants.STANDARD_DIMENSIONS
        assert body["metrics"] == constants.STANDARD_METRICS
        assert [p.url.path for p in polls] == ["/api/reports/status/r-1"] * 2
        assert download.url.host == "storage.example"
        assert "authorization" not in download.headers

    def test_invalid_request_raises(self, monkeypatch: Any):
        with pytest.raises(RuntimeError, match="rejected"):
            self._run(monkeypatch, [], run={"valid": False})

    def test_failed_report_raises(self, monkeypatch: Any):
        with pytest.raises(RuntimeError, match="failed"):
            self._run(monkeypatch, [{"status": "error"}])
