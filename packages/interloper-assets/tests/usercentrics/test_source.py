"""Regression tests for the Usercentrics source.

Both assets read the same data-export endpoint, which answers with CSV download
URLs whose headers are already snake case. These tests pin the asset set, the
normalizer, the spec round-trip, reconciliation of vendor-shaped rows, the
partition column, the analytics-ID discriminator, and the export helper against
a faked API.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.usercentrics import constants, schemas
from interloper_assets.usercentrics.connection import UsercentricsConnection
from interloper_assets.usercentrics.source import Usercentrics

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
    return Usercentrics(id="src-1", analytics_id="an-1")


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
        assert source.asset_table(_asset("consents_stats")) == "consents_stats__an-1"

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
        return Representation.of(normalized).reconcile(ASSETS[key]).loc[0]

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


class TestExport:
    def _export(self, monkeypatch: Any, urls: list[str], files: dict[str, httpx2.Response]) -> Any:
        requests: list[httpx2.Request] = []

        def api(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"downloadUrls": urls})

        def storage(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return files[str(request.url)]

        connection = UsercentricsConnection(api_key="secret")
        monkeypatch.setitem(
            connection.__dict__,
            "client",
            il.AsyncRESTClient(
                constants.BASE_URL, headers={"X-API-Key": "secret"}, transport=httpx2.MockTransport(api)
            ),
        )
        real_client = httpx2.AsyncClient
        monkeypatch.setattr(
            httpx2, "AsyncClient", lambda **kwargs: real_client(transport=httpx2.MockTransport(storage), **kwargs)
        )
        source: Any = Usercentrics(id="src-1", analytics_id="an-1", connection=connection)
        rows = asyncio.run(source._export("granular", dt.date(2026, 6, 14)))
        return rows, requests

    def test_concatenates_every_file(self, monkeypatch: Any):
        urls = ["https://storage.example/1.csv", "https://storage.example/2.csv"]
        rows, requests = self._export(
            monkeypatch,
            urls,
            {urls[0]: httpx2.Response(200, text=GRANULAR_CSV), urls[1]: httpx2.Response(200, text=GRANULAR_CSV)},
        )
        assert len(rows) == 2 and rows[0]["dps_id"] == "HkocEodjb7"
        export, *downloads = requests
        assert str(export.url) == f"{constants.BASE_URL}/analytics/an-1/granular-2026-06-14"
        assert export.headers["x-api-key"] == "secret"
        assert all("x-api-key" not in d.headers for d in downloads)

    def test_empty_file_yields_no_rows(self, monkeypatch: Any):
        url = "https://storage.example/1.csv"
        rows, _ = self._export(monkeypatch, [url], {url: httpx2.Response(200, text="")})
        assert rows == []

    def test_failed_download_raises(self, monkeypatch: Any):
        url = "https://storage.example/1.csv"
        with pytest.raises(httpx2.HTTPStatusError):
            self._export(monkeypatch, [url], {url: httpx2.Response(403)})
