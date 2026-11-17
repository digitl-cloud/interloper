"""Regression tests for the Usercentrics source.

Both assets read the same data-export endpoint, which answers with CSV download
URLs whose headers are already snake case. These tests pin the asset set, the
normalizer, the spec round-trip, reconciliation of vendor-shaped rows, the
partition column, the analytics-ID discriminator, and what each asset asks the
engine for and returns against a faked API.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.usercentrics import constants
from interloper_assets.usercentrics.connection import UsercentricsConnection
from interloper_assets.usercentrics.connector import schemas
from interloper_assets.usercentrics.connector.source import Usercentrics

GRANULAR_CSV = (
    "day,settings_id,dps_id,consent,action,total,country,os,browser\n"
    "2026-06-14,abc,HkocEodjb7,12,15,20,DE,Mac OS,Chrome\n"
)
INTERACTION_CSV = (
    "day,event_type,settings_id,host,country,browser,device_type,os,number\n"
    "2026-06-14,3,abc,www.example.com,,Firefox,desktop,Windows,57\n"
)

ASSETS = {"consents_stats": schemas.ConsentsStats, "interactions_stats": schemas.InteractionsStats}


def _source() -> Any:
    return Usercentrics(id="src-1", analytics_id="an-1", connection=UsercentricsConnection(api_key="x"))


def _asset(key: str) -> Any:
    return next(a for a in _source().assets if type(a).key == key)


class TestAssets:
    def test_asset_key_set(self):
        assert {type(a).key for a in _source().assets} == set(ASSETS)

    def test_assets_use_the_source_normalizer(self):
        for key in ASSETS:
            normalizer = _asset(key).normalizer
            assert isinstance(normalizer, DataFrameNormalizer), key
            assert normalizer.replace_empty_strings is True, key

    def test_analytics_id_discriminates_the_tables(self):
        source = _source()
        assert source.discriminator == "an-1"
        assert source.table(_asset("consents_stats")) == "consents_stats__an_1"

    def test_analytics_id_is_no_longer_a_connection_field(self):
        assert "analytics_id" not in UsercentricsConnection.model_fields


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        source = _source()
        asset = next(a for a in source.assets if type(a).key == key)
        spec_json = DAG(source).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def _reconcile(self, key: str, text: str) -> Any:
        child = self._child(key)
        assert isinstance(child.normalizer, DataFrameNormalizer)
        normalized = child.normalizer.normalize(list(csv.DictReader(text.splitlines())))
        assert set(normalized.columns) <= set(ASSETS[key].model_fields)
        return TableData.from_data(normalized).reconcile(ASSETS[key]).loc[0]

    def test_granular_row_reconciles(self):
        row = self._reconcile("consents_stats", GRANULAR_CSV)
        assert row["day"] == dt.date(2026, 6, 14)
        assert (int(row["consent"]), int(row["action"]), int(row["total"])) == (12, 15, 20)
        assert row["dps_id"] == "HkocEodjb7"

    def test_interaction_row_reconciles(self):
        row = self._reconcile("interactions_stats", INTERACTION_CSV)
        assert row["day"] == dt.date(2026, 6, 14)
        assert int(row["event_type"]) == 3 and int(row["number"]) == 57


class TestPartitionColumns:
    def test_reports_partition_on_the_vendor_day(self):
        for key in ASSETS:
            asset = _asset(key)
            assert asset.tags == ["Report"], key
            assert asset.partitioning is not None and asset.partitioning.column == "day", key
            assert asset.schema is not None and "date" not in asset.schema.model_fields, key


class TestRequests:
    URL = "https://storage.example/1.csv"

    def _run(self, monkeypatch: Any, key: str, text: str) -> tuple[list[dict[str, Any]], list[httpx2.Request]]:
        requests: list[httpx2.Request] = []

        def api(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"downloadUrls": [self.URL]})

        def storage(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, text=text)

        connection = UsercentricsConnection(api_key="secret")
        connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(api))
        real_client = httpx2.AsyncClient
        monkeypatch.setattr(
            httpx2, "AsyncClient", lambda **kwargs: real_client(transport=httpx2.MockTransport(storage), **kwargs)
        )
        source = Usercentrics(id="src-1", analytics_id="an-1", connection=connection)
        asset = next(a for a in source.assets if type(a).key == key)
        context = il.ExecutionContext(asset, il.TimePartition(value=dt.date(2026, 6, 14)).window)
        return asyncio.run(asset.data(context=context)), requests

    @pytest.mark.parametrize(
        ("key", "aggregation", "text"),
        [("consents_stats", "granular", GRANULAR_CSV), ("interactions_stats", "interaction", INTERACTION_CSV)],
    )
    def test_each_asset_exports_its_aggregation_for_its_day(
        self, monkeypatch: Any, key: str, aggregation: str, text: str
    ):
        rows, requests = self._run(monkeypatch, key, text)

        export, download = requests
        assert (export.method, str(export.url)) == (
            "GET",
            f"{constants.BASE_URL}/analytics/an-1/{aggregation}-2026-06-14",
        )
        assert export.content == b""
        assert str(download.url) == self.URL
        assert rows == list(csv.DictReader(text.splitlines()))
