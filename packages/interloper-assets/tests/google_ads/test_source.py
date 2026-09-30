"""Regression tests for the Google Ads source.

The SDK streams proto-plus ``GoogleAdsRow`` messages; the source converts each
through ``MessageToDict`` into the REST JSON shape (camelCase keys nested per
resource, int64 values as strings, ``type`` rather than proto-plus's ``type_``)
and the source normalizer flattens that into full GAQL resource paths
(``metrics_clicks``, ``ad_group_ad_ad_type``, ``segments_keyword_info_text``).
These tests pin the asset set, the normalizer, the spec round-trip, the query
the stream is asked for, reconciliation of SDK-built rows, and the partition
column (the vendor's ``segments_date``).
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any

import interloper as il
import pytest
from interloper.asset.context import ExecutionContext
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.google_ads import constants, schemas
from interloper_assets.google_ads.connection import GoogleAdsConnection
from interloper_assets.google_ads.source import GoogleAds

ASSET_KEYS = ("campaigns_stats", "ads_stats")
DAY = dt.date(2026, 6, 14)


def _campaign_row() -> Any:
    from google.ads.googleads.v23.services.types.google_ads_service import GoogleAdsRow

    row = GoogleAdsRow()
    row.campaign.resource_name = "customers/123/campaigns/456"
    row.campaign.id = 456
    row.campaign.name = "Summer"
    row.campaign.status = "ENABLED"
    row.campaign.advertising_channel_type = "SEARCH"
    row.campaign.bidding_strategy_type = "TARGET_CPA"
    row.campaign.start_date_time = "2026-02-18 00:00:00"
    row.campaign_budget.amount_micros = 50_000_000
    row.customer.id = 123
    row.customer.descriptive_name = "Acme"
    row.metrics.clicks = 42
    row.metrics.impressions = 1000
    row.metrics.cost_micros = 12_500_000
    row.metrics.ctr = 0.042
    row.metrics.conversions = 1.5
    row.metrics.search_absolute_top_impression_share = 0.31
    row.metrics.trueview_average_cpv = 12345.6
    row.segments.date = DAY.isoformat()
    return row


def _ad_row() -> Any:
    from google.ads.googleads.v23.services.types.google_ads_service import GoogleAdsRow

    row = GoogleAdsRow()
    row.ad_group_ad.resource_name = "customers/123/adGroupAds/789~111"
    row.ad_group_ad.ad.resource_name = "customers/123/ads/111"
    row.ad_group_ad.ad.id = 111
    row.ad_group_ad.ad.type_ = "RESPONSIVE_SEARCH_AD"
    row.ad_group_ad.ad.final_urls.extend(["https://example.com/a", "https://example.com/b"])
    row.ad_group.id = 789
    row.campaign.id = 456
    row.campaign.network_settings.target_search_network = True
    row.campaign.network_settings.target_partner_search_network = False
    row.metrics.clicks = 3
    row.segments.date = DAY.isoformat()
    row.segments.device = "MOBILE"
    row.segments.click_type = "URL_CLICKS"
    row.segments.keyword.ad_group_criterion = "customers/123/adGroupCriteria/789~222"
    row.segments.keyword.info.text = "running shoes"
    row.segments.keyword.info.match_type = "EXACT"
    return row


class _FakeService:
    def __init__(self, rows: list[Any]):
        self.rows = rows
        self.calls: list[dict[str, Any]] = []

    def search_stream(self, **kwargs: Any) -> list[Any]:
        self.calls.append(kwargs)
        return [SimpleNamespace(results=self.rows[:1]), SimpleNamespace(results=self.rows[1:])]


def _source(monkeypatch: Any | None = None, service: _FakeService | None = None, **fields: Any) -> Any:
    connection = GoogleAdsConnection(
        client_id="cid", client_secret="secret", refresh_token="token", developer_token="dev"
    )
    if monkeypatch is not None and service is not None:
        monkeypatch.setitem(connection.__dict__, "client", SimpleNamespace(get_service=lambda name: service))
    return GoogleAds(id="src-1", connection=connection, customer_id="123", **fields)


def _asset(key: str, source: Any = None) -> Any:
    return next(a for a in (source or _source()).assets if type(a).key == key)


def _run(asset: Any) -> list[dict[str, Any]]:
    context = ExecutionContext(
        asset_key=asset.key,
        partitioning=asset.partitioning,
        partition_or_window=il.TimePartition(value=DAY),
    )
    return asset.data(context=context)


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


class TestSearchStream:
    """Each asset asks the stream for its one-day GAQL query, through the manager when set."""

    def test_campaigns_query_and_login_customer(self, monkeypatch: Any):
        service = _FakeService([_campaign_row(), _campaign_row()])
        asset = _asset("campaigns_stats", _source(monkeypatch, service, login_customer_id="999"))
        rows = _run(asset)
        assert len(rows) == 2
        (call,) = service.calls
        assert call["customer_id"] == "123"
        assert call["metadata"] == [("login-customer-id", "999")]
        assert call["query"].startswith(f"SELECT {', '.join(constants.CAMPAIGN_FIELDS)} FROM campaign ")
        assert call["query"].endswith("WHERE segments.date BETWEEN '2026-06-14' AND '2026-06-14'")

    def test_no_login_customer_sends_no_metadata(self, monkeypatch: Any):
        service = _FakeService([_ad_row()])
        _run(_asset("ads_stats", _source(monkeypatch, service)))
        assert service.calls[0]["metadata"] == []
        assert " FROM ad_group_ad " in service.calls[0]["query"]

    def test_rows_take_the_rest_json_shape(self, monkeypatch: Any):
        service = _FakeService([_ad_row()])
        (row,) = _run(_asset("ads_stats", _source(monkeypatch, service)))
        assert row["adGroupAd"]["ad"]["type"] == "RESPONSIVE_SEARCH_AD"
        assert row["adGroupAd"]["ad"]["id"] == "111"
        assert row["segments"]["keyword"]["info"]["text"] == "running shoes"


class TestReconcile:
    """SDK-built rows normalize onto the full resource paths with the declared types."""

    def _reconcile(self, monkeypatch: Any, key: str, row: Any, schema: Any) -> Any:
        asset = _asset(key, _source(monkeypatch, _FakeService([row])))
        normalized = asset.normalizer.normalize(_run(asset))
        return Representation.of(normalized).reconcile(schema)

    def test_campaign_row(self, monkeypatch: Any):
        reconciled = self._reconcile(monkeypatch, "campaigns_stats", _campaign_row(), schemas.CampaignsStats)
        row = reconciled.iloc[0]
        assert row["campaign_id"] == "456"
        assert row["campaign_resource_name"] == "customers/123/campaigns/456"
        assert row["campaign_status"] == "ENABLED"
        assert int(row["campaign_budget_amount_micros"]) == 50_000_000
        assert int(row["metrics_clicks"]) == 42
        assert int(row["metrics_cost_micros"]) == 12_500_000
        assert float(row["metrics_ctr"]) == pytest.approx(0.042)
        assert float(row["metrics_trueview_average_cpv"]) == pytest.approx(12345.6)
        assert float(row["metrics_search_absolute_top_impression_share"]) == pytest.approx(0.31)
        assert row["campaign_start_date_time"].to_pydatetime() == dt.datetime(2026, 2, 18)
        assert row["segments_date"] == DAY

    def test_ad_row(self, monkeypatch: Any):
        reconciled = self._reconcile(monkeypatch, "ads_stats", _ad_row(), schemas.AdsStats)
        row = reconciled.iloc[0]
        assert row["ad_group_ad_ad_type"] == "RESPONSIVE_SEARCH_AD"
        assert row["ad_group_ad_ad_id"] == "111"
        assert row["ad_group_ad_resource_name"] == "customers/123/adGroupAds/789~111"
        assert row["ad_group_ad_ad_resource_name"] == "customers/123/ads/111"
        assert row["ad_group_ad_ad_final_urls"] == '["https://example.com/a", "https://example.com/b"]'
        assert row["ad_group_id"] == "789"
        assert bool(row["campaign_network_settings_target_search_network"]) is True
        assert bool(row["campaign_network_settings_target_partner_search_network"]) is False
        assert row["segments_device"] == "MOBILE"
        assert row["segments_click_type"] == "URL_CLICKS"
        assert row["segments_keyword_ad_group_criterion"] == "customers/123/adGroupCriteria/789~222"
        assert row["segments_keyword_info_text"] == "running shoes"
        assert row["segments_keyword_info_match_type"] == "EXACT"
        assert row["segments_date"] == DAY


class TestPartitionColumns:
    """GAQL rows carry the report day as ``segments.date``; nothing is stamped."""

    @pytest.mark.parametrize("key", ASSET_KEYS)
    def test_partitions_on_segments_date(self, key: str):
        asset = _asset(key)
        assert asset.tags == ["Report"]
        assert asset.partitioning is not None and asset.partitioning.column == "segments_date"
        assert asset.schema is not None
        assert "segments_date" in asset.schema.model_fields
        assert "date" not in asset.schema.model_fields
