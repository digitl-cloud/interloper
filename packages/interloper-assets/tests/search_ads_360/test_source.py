"""Regression tests for the Search Ads 360 source.

``searchAds360:search`` returns GAQL rows as REST JSON: camelCase keys nested
per resource, int64 values as strings, enums by name. The source normalizer
flattens them into full resource paths (``metrics_cost_micros``,
``customer_client_descriptive_name``). These tests pin the asset set, the
normalizer, the spec round-trip, the paged POST flow against a faked API,
reconciliation of vendor-shaped rows, and the partition columns (the vendor's
``segments_date`` for the report, a stamped ``date`` for the entity).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.asset.context import ExecutionContext
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_ads_360 import constants, schemas
from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.source import SearchAds360

ASSET_KEYS = ("campaigns_stats", "customer_clients")
DAY = dt.date(2026, 6, 14)

CAMPAIGN_ROW = {
    "campaign": {
        "resourceName": "customers/222/campaigns/456",
        "name": "Summer",
        "id": "456",
        "status": "ENABLED",
        "advertisingChannelType": "SEARCH",
        "biddingStrategyType": "MANUAL_CPC",
    },
    "customer": {"resourceName": "customers/222", "id": "222", "accountType": "GOOGLE_ADS"},
    "segments": {"date": "2026-06-14"},
    "metrics": {
        "impressions": "1000",
        "averageCost": 297619.05,
        "clicks": "42",
        "ctr": 0.042,
        "averageCpc": 297619.05,
        "costMicros": "12500000",
    },
}

CUSTOMER_CLIENT_ROW = {
    "customer": {"resourceName": "customers/111", "id": "111", "descriptiveName": "Acme MCC"},
    "customerClient": {
        "resourceName": "customers/111/customerClients/222",
        "descriptiveName": "Acme DE",
        "id": "222",
        "status": "ENABLED",
        "level": "1",
        "currencyCode": "EUR",
        "manager": False,
    },
}


def _source(connection: SearchAds360Connection | None = None) -> Any:
    return SearchAds360(
        id="src-1",
        connection=connection or SearchAds360Connection(service_account_key="{}"),
        manager_customer_id="111",
        customer_client_id="222",
    )


def _asset(key: str, source: Any = None) -> Any:
    return next(a for a in (source or _source()).assets if type(a).key == key)


class TestAssets:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == set(ASSET_KEYS)

    @pytest.mark.parametrize("key", ASSET_KEYS)
    def test_uses_the_source_normalizer(self, key: str):
        normalizer = _asset(key).normalizer
        assert type(normalizer) is DataFrameNormalizer
        assert normalizer.flatten_max_level == 3
        assert normalizer.drop_na_columns is True

    @pytest.mark.parametrize("key", ASSET_KEYS)
    def test_spec_roundtrip_keeps_the_normalizer(self, key: str):
        source = _source()
        spec = DAG(source).mini_dag(_asset(key, source).id).to_spec().model_dump(mode="json")
        child: Any = next(a for a in DAGSpec(**spec).reconstruct().operations if type(a).key == key)
        assert type(child.normalizer) is DataFrameNormalizer
        assert child.normalizer.flatten_max_level == 3


class TestSearch:
    """POST the query, follow ``nextPageToken`` in the body, route through the manager where the report needs it."""

    def _run(self, monkeypatch: Any, key: str, pages: list[dict[str, Any]]) -> tuple[list[Any], list[httpx2.Request]]:
        requests: list[httpx2.Request] = []
        remaining = list(pages)

        def api(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json=remaining.pop(0))

        connection = SearchAds360Connection(service_account_key="{}")
        monkeypatch.setitem(
            connection.__dict__,
            "client",
            il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(api)),
        )
        asset = _asset(key, _source(connection))
        context = ExecutionContext(
            asset_key=asset.key,
            partitioning=asset.partitioning,
            partition_or_window=il.TimePartition(value=DAY),
        )
        return asyncio.run(asset.data(context=context)), requests

    def test_campaigns_follow_the_page_token(self, monkeypatch: Any):
        rows, requests = self._run(
            monkeypatch,
            "campaigns_stats",
            [{"results": [CAMPAIGN_ROW], "nextPageToken": "p2"}, {"results": [CAMPAIGN_ROW]}],
        )
        assert len(rows) == 2
        first, second = (json.loads(r.content) for r in requests)
        assert first == {
            "query": (
                f"SELECT {', '.join(constants.CAMPAIGN_FIELDS)} FROM campaign"
                " WHERE segments.date BETWEEN '2026-06-14' AND '2026-06-14'"
            )
        }
        assert second == {**first, "pageToken": "p2"}
        for request in requests:
            assert request.method == "POST"
            assert request.url.path == "/v0/customers/222/searchAds360:search"
            assert request.headers["login-customer-id"] == "111"

    def test_empty_result_pages_yield_no_rows(self, monkeypatch: Any):
        rows, _ = self._run(monkeypatch, "campaigns_stats", [{"fieldMask": "campaign.name"}])
        assert rows == []

    def test_customer_clients_query_the_manager_directly(self, monkeypatch: Any):
        rows, (request,) = self._run(monkeypatch, "customer_clients", [{"results": [CUSTOMER_CLIENT_ROW]}])
        assert request.url.path == "/v0/customers/111/searchAds360:search"
        assert "login-customer-id" not in request.headers
        assert json.loads(request.content) == {
            "query": f"SELECT {', '.join(constants.CUSTOMER_CLIENT_FIELDS)} FROM customer_client"
        }
        assert rows[0]["date"] == DAY


class TestReconcile:
    """REST rows normalize onto the full resource paths with the declared types."""

    def test_campaign_row(self):
        normalized = _asset("campaigns_stats").normalizer.normalize([CAMPAIGN_ROW])
        row = Representation.of(normalized).reconcile(schemas.CampaignsStats).iloc[0]
        assert row["campaign_id"] == "456"
        assert row["campaign_advertising_channel_type"] == "SEARCH"
        assert row["customer_account_type"] == "GOOGLE_ADS"
        assert row["customer_resource_name"] == "customers/222"
        assert int(row["metrics_clicks"]) == 42
        assert int(row["metrics_impressions"]) == 1000
        assert int(row["metrics_cost_micros"]) == 12_500_000
        assert float(row["metrics_average_cpc"]) == pytest.approx(297619.05)
        assert float(row["metrics_ctr"]) == pytest.approx(0.042)
        assert row["segments_date"] == DAY

    def test_customer_client_row(self):
        normalized = _asset("customer_clients").normalizer.normalize([{**CUSTOMER_CLIENT_ROW, "date": DAY}])
        row = Representation.of(normalized).reconcile(schemas.CustomerClients).iloc[0]
        assert row["customer_id"] == "111"
        assert row["customer_descriptive_name"] == "Acme MCC"
        assert row["customer_client_id"] == "222"
        assert row["customer_client_descriptive_name"] == "Acme DE"
        assert row["customer_client_resource_name"] == "customers/111/customerClients/222"
        assert int(row["customer_client_level"]) == 1
        assert bool(row["customer_client_manager"]) is False
        assert row["customer_client_currency_code"] == "EUR"
        assert row["date"] == DAY


class TestPartitionColumns:
    def test_report_partitions_on_segments_date(self):
        asset = _asset("campaigns_stats")
        assert asset.tags == ["Report"]
        assert asset.partitioning is not None and asset.partitioning.column == "segments_date"
        assert asset.schema is not None
        assert "segments_date" in asset.schema.model_fields
        assert "date" not in asset.schema.model_fields

    def test_entity_stamps_a_date(self):
        asset = _asset("customer_clients")
        assert asset.tags == ["Entity"]
        assert asset.partitioning is not None and asset.partitioning.column == "date"
        assert asset.schema is not None and "date" in asset.schema.model_fields
