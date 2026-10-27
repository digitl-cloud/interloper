"""Regression tests for the Criteo source configuration.

The Criteo statistics report returns PascalCase columns (``Day``, ``AdId``,
``SalesAllClientAttribution``, …) while the schemas are snake_case. A
source-level ``DataFrameNormalizer`` bridges the two — these tests pin that
the normalizer reaches every asset instance, survives the host→child spec
round-trip, and that a raw API-shaped row normalizes and validates against
the schema (the same chain that broke in prod for AmazonAds; see
``amazon_ads/test_source.py``).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pandas as pd
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.criteo import constants
from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.connector.schemas import AdsStats, CampaignsStats
from interloper_assets.criteo.connector.source import Criteo

# Raw API column names per asset, as Criteo returns them: the requested id
# dimensions, their companion name/currency columns, and the full metric set.
ADS_COLUMNS = [
    "Day",
    "AdId",
    "Ad",
    "AdsetId",
    "Adset",
    "CampaignId",
    "Campaign",
    "Currency",
    *constants.STATISTICS_METRICS,
]
CAMPAIGNS_COLUMNS = ["Day", "CampaignId", "Campaign", "Currency", *constants.STATISTICS_METRICS]


def _source() -> Any:
    return Criteo(id="src-1", advertiser_id="123")  # ty: ignore[missing-argument]


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer and conform."""

    def _child_asset(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_ads_row_conforms_after_roundtrip(self):
        child = self._child_asset("ads_stats")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        row: dict[str, object] = dict.fromkeys(ADS_COLUMNS)
        row["Day"] = "2026-06-10"
        normalized = child.normalizer.normalize(pd.DataFrame([row]))
        TableData.from_data(normalized).reconcile(AdsStats)  # must not raise

    def test_campaigns_row_conforms_after_roundtrip(self):
        child = self._child_asset("campaigns_stats")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        row: dict[str, object] = dict.fromkeys(CAMPAIGNS_COLUMNS)
        row["Day"] = "2026-06-10"
        normalized = child.normalizer.normalize(pd.DataFrame([row]))
        TableData.from_data(normalized).reconcile(CampaignsStats)  # must not raise


class TestApiVersion:
    """Every call goes to the supported API version; 2025-04 is past Criteo's support window."""

    def test_the_reports_and_the_advertiser_list_call_version_2026_07(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.path.endswith("/advertisers/me"):
                return httpx2.Response(200, json={"data": [{"id": "123", "attributes": {"advertiserName": "Example"}}]})
            return httpx2.Response(200, json={"Rows": [{"Day": "2026-10-26", "CampaignId": "7"}]})

        connection = CriteoConnection(id="criteo", client_id="cid", client_secret="x", refresh_token="x")
        connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
        source = Criteo(id="src-1", advertiser_id="123", connection=connection)
        asset = next(a for a in source.assets if type(a).key == "campaigns_stats")
        partition = il.TimePartition(value=dt.date(2026, 10, 26))

        rows = asyncio.run(asset.data(context=il.ExecutionContext(asset, partition.window)))
        advertisers = asyncio.run(connection.advertisers())

        assert [request.url.path for request in requests] == ["/2026-07/statistics/report", "/2026-07/advertisers/me"]
        assert json.loads(requests[0].content)["dimensions"] == ["Day", "CampaignId"]
        assert rows == [{"Day": "2026-10-26", "CampaignId": "7"}]
        assert advertisers == [{"id": "123", "name": "Example"}]
