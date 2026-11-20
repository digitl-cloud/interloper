"""Regression tests for the Google Ads source.

Google answers GAQL in the REST JSON shape: camelCase keys nested per resource,
int64 values as strings, enums by name, unset fields omitted, and doubles as
JSON numbers, so a whole one has no fraction (``"conversions": 2``). The source
normalizer flattens that into full GAQL resource paths (``metrics_clicks``,
``ad_group_ad_ad_type``, ``segments_keyword_info_text``) and the schema types
it. These tests pin the asset set, the normalizer, the spec round-trip, each
asset's request (customer, query, manager account) and the rows it writes, and
the partition column (the vendor's ``segments_date``).
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.asset.context import ExecutionContext
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.google_ads import constants
from interloper_assets.google_ads.connection import GoogleAdsConnection
from interloper_assets.google_ads.connector import schemas
from interloper_assets.google_ads.connector.source import GoogleAds

ASSET_KEYS = ("campaigns_stats", "ads_stats")
DAY = dt.date(2026, 6, 14)
WINDOW = "segments.date BETWEEN '2026-06-14' AND '2026-06-14'"

CUSTOMER = {"resourceName": "customers/123", "id": "123", "descriptiveName": "Acme"}
CAMPAIGN = {
    "resourceName": "customers/123/campaigns/456",
    "status": "ENABLED",
    "advertisingChannelType": "SEARCH",
    "biddingStrategyType": "TARGET_CPA",
    "name": "Summer",
    "id": "456",
    "startDateTime": "2026-02-18 00:00:00",
    "endDateTime": "2037-12-30 23:59:59",
}
CAMPAIGN_ROW = {
    "customer": CUSTOMER,
    "campaign": CAMPAIGN,
    "campaignBudget": {"resourceName": "customers/123/campaignBudgets/789", "amountMicros": "50000000"},
    "metrics": {
        "clicks": "42",
        "conversionsValue": 120.5,
        "conversions": 2,
        "costMicros": "12500000",
        "ctr": 0.042,
        "averageCost": 297619.04761904763,
        "averageCpc": 297619.04761904763,
        "averageCpm": 12500000,
        "impressions": "1000",
        "interactions": "42",
        "interactionRate": 0.042,
        "searchImpressionShare": 0.5,
        "searchAbsoluteTopImpressionShare": 0.31,
        "allConversions": 2.5,
        "allConversionsValue": 130,
        "costPerAllConversions": 5000000,
        "conversionsFromInteractionsRate": 0.047619047619047616,
        "trueviewAverageCpv": 12345.6,
    },
    "segments": {"date": "2026-06-14"},
}
AD_ROW = {
    "customer": CUSTOMER,
    "campaign": {**CAMPAIGN, "networkSettings": {"targetSearchNetwork": True, "targetPartnerSearchNetwork": False}},
    "adGroup": {"resourceName": "customers/123/adGroups/789", "id": "789", "name": "Shoes"},
    "adGroupAd": {
        "resourceName": "customers/123/adGroupAds/789~111",
        "ad": {
            "resourceName": "customers/123/ads/111",
            "id": "111",
            "type": "RESPONSIVE_SEARCH_AD",
            "finalUrls": ["https://example.com/a", "https://example.com/b"],
        },
    },
    "metrics": {
        "clicks": "3",
        "conversionsValue": 40,
        "conversions": 1,
        "costMicros": "900000",
        "ctr": 0.03,
        "averageCost": 300000,
        "averageCpc": 300000,
        "averageCpm": 9000000,
        "impressions": "100",
        "interactions": "3",
        "interactionRate": 0.03,
        "allConversions": 1.5,
        "allConversionsValue": 45.5,
        "costPerAllConversions": 600000,
        "conversionsFromInteractionsRate": 0.3333333333333333,
    },
    "segments": {
        "date": "2026-06-14",
        "device": "MOBILE",
        "clickType": "URL_CLICKS",
        "keyword": {
            "adGroupCriterion": "customers/123/adGroupCriteria/789~222",
            "info": {"text": "running shoes", "matchType": "EXACT"},
        },
    },
}

CAMPAIGN_COLUMNS = {
    "campaign_advertising_channel_type": ("string", "SEARCH"),
    "campaign_bidding_strategy_type": ("string", "TARGET_CPA"),
    "campaign_budget_amount_micros": ("Int64", 50_000_000),
    "campaign_budget_resource_name": ("string", "customers/123/campaignBudgets/789"),
    "campaign_end_date_time": ("datetime64[us]", dt.datetime(2037, 12, 30, 23, 59, 59)),
    "campaign_id": ("string", "456"),
    "campaign_name": ("string", "Summer"),
    "campaign_resource_name": ("string", "customers/123/campaigns/456"),
    "campaign_start_date_time": ("datetime64[us]", dt.datetime(2026, 2, 18)),
    "campaign_status": ("string", "ENABLED"),
    "customer_descriptive_name": ("string", "Acme"),
    "customer_id": ("string", "123"),
    "customer_resource_name": ("string", "customers/123"),
    "metrics_all_conversions": ("Float64", 2.5),
    "metrics_all_conversions_value": ("Float64", 130.0),
    "metrics_average_cost": ("Float64", 297619.04761904763),
    "metrics_average_cpc": ("Float64", 297619.04761904763),
    "metrics_average_cpm": ("Float64", 12_500_000.0),
    "metrics_clicks": ("Int64", 42),
    "metrics_conversions": ("Float64", 2.0),
    "metrics_conversions_from_interactions_rate": ("Float64", 0.047619047619047616),
    "metrics_conversions_value": ("Float64", 120.5),
    "metrics_cost_micros": ("Int64", 12_500_000),
    "metrics_cost_per_all_conversions": ("Float64", 5_000_000.0),
    "metrics_ctr": ("Float64", 0.042),
    "metrics_impressions": ("Int64", 1000),
    "metrics_interaction_rate": ("Float64", 0.042),
    "metrics_interactions": ("Int64", 42),
    "metrics_search_absolute_top_impression_share": ("Float64", 0.31),
    "metrics_search_impression_share": ("Float64", 0.5),
    "metrics_trueview_average_cpv": ("Float64", 12345.6),
    "segments_date": ("object", DAY),
}
AD_COLUMNS = {
    "ad_group_ad_ad_final_urls": ("string", '["https://example.com/a", "https://example.com/b"]'),
    "ad_group_ad_ad_id": ("string", "111"),
    "ad_group_ad_ad_name": ("string", None),
    "ad_group_ad_ad_resource_name": ("string", "customers/123/ads/111"),
    "ad_group_ad_ad_type": ("string", "RESPONSIVE_SEARCH_AD"),
    "ad_group_ad_resource_name": ("string", "customers/123/adGroupAds/789~111"),
    "ad_group_id": ("string", "789"),
    "ad_group_name": ("string", "Shoes"),
    "ad_group_resource_name": ("string", "customers/123/adGroups/789"),
    "campaign_advertising_channel_type": ("string", "SEARCH"),
    "campaign_bidding_strategy_type": ("string", "TARGET_CPA"),
    "campaign_end_date_time": ("datetime64[us]", dt.datetime(2037, 12, 30, 23, 59, 59)),
    "campaign_id": ("string", "456"),
    "campaign_name": ("string", "Summer"),
    "campaign_network_settings_target_partner_search_network": ("boolean", False),
    "campaign_network_settings_target_search_network": ("boolean", True),
    "campaign_resource_name": ("string", "customers/123/campaigns/456"),
    "campaign_start_date_time": ("datetime64[us]", dt.datetime(2026, 2, 18)),
    "campaign_status": ("string", "ENABLED"),
    "customer_descriptive_name": ("string", "Acme"),
    "customer_id": ("string", "123"),
    "customer_resource_name": ("string", "customers/123"),
    "metrics_all_conversions": ("Float64", 1.5),
    "metrics_all_conversions_value": ("Float64", 45.5),
    "metrics_average_cost": ("Float64", 300_000.0),
    "metrics_average_cpc": ("Float64", 300_000.0),
    "metrics_average_cpm": ("Float64", 9_000_000.0),
    "metrics_clicks": ("Int64", 3),
    "metrics_conversions": ("Float64", 1.0),
    "metrics_conversions_from_interactions_rate": ("Float64", 0.3333333333333333),
    "metrics_conversions_value": ("Float64", 40.0),
    "metrics_cost_micros": ("Int64", 900_000),
    "metrics_cost_per_all_conversions": ("Float64", 600_000.0),
    "metrics_ctr": ("Float64", 0.03),
    "metrics_impressions": ("Int64", 100),
    "metrics_interaction_rate": ("Float64", 0.03),
    "metrics_interactions": ("Int64", 3),
    "segments_click_type": ("string", "URL_CLICKS"),
    "segments_date": ("object", DAY),
    "segments_device": ("string", "MOBILE"),
    "segments_keyword_ad_group_criterion": ("string", "customers/123/adGroupCriteria/789~222"),
    "segments_keyword_info_match_type": ("string", "EXACT"),
    "segments_keyword_info_text": ("string", "running shoes"),
}


class _Google:
    """Google Ads, answering every query with ``rows`` and recording each as (customer, query, manager account)."""

    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows
        self.sent: list[tuple[str, str, str | None]] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        if str(request.url) == "https://oauth2.googleapis.com/token":
            return httpx2.Response(200, json={"access_token": "access"})
        prefix, customer_id, method = request.url.path.rsplit("/", 2)
        assert (request.method, prefix, method) == (
            "POST",
            f"/{constants.API_VERSION}/customers",
            "googleAds:searchStream",
        )
        self.sent.append((customer_id, json.loads(request.content)["query"], request.headers.get("login-customer-id")))
        return httpx2.Response(200, json=[{"results": self.rows, "fieldMask": "customer.id", "requestId": "r1"}])


def _source(google: _Google | None = None, **fields: Any) -> Any:
    connection = GoogleAdsConnection(
        client_id="cid", client_secret="secret", refresh_token="token", developer_token="dev"
    )
    if google is not None:
        connection.client._transport = httpx2.MockTransport(google)
    return GoogleAds(id="src-1", connection=connection, customer_id="123", **fields)


def _asset(key: str, source: Any = None) -> Any:
    return next(a for a in (source or _source()).assets if type(a).key == key)


async def _run(asset: Any) -> list[dict[str, Any]]:
    context = ExecutionContext(asset, il.TimePartition(value=DAY).window)
    return await asset.data(context=context)


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


class TestRequests:
    """Each asset reads its one-day GAQL query for the source's customer, through the manager account when set."""

    async def test_campaigns_stats(self):
        google = _Google([CAMPAIGN_ROW])
        await _run(_asset("campaigns_stats", _source(google)))
        query = f"SELECT {', '.join(constants.CAMPAIGN_FIELDS)} FROM campaign WHERE {WINDOW}"
        assert google.sent == [("123", query, None)]

    async def test_ads_stats_through_a_manager_account(self):
        google = _Google([AD_ROW])
        await _run(_asset("ads_stats", _source(google, login_customer_id="999")))
        query = f"SELECT {', '.join(constants.AD_FIELDS)} FROM ad_group_ad WHERE {WINDOW}"
        assert google.sent == [("123", query, "999")]


class TestRows:
    """Google's rows reach the asset as Google sends them, and are written as the schema types them."""

    @pytest.mark.parametrize(
        ("key", "row", "schema", "columns"),
        [
            ("campaigns_stats", CAMPAIGN_ROW, schemas.CampaignsStats, CAMPAIGN_COLUMNS),
            ("ads_stats", AD_ROW, schemas.AdsStats, AD_COLUMNS),
        ],
    )
    async def test_rows_written(self, key: str, row: dict[str, Any], schema: Any, columns: dict[str, tuple[str, Any]]):
        asset = _asset(key, _source(_Google([row])))
        rows = await _run(asset)
        assert rows == [row]

        reconciled = TableData.from_data(asset.normalizer.normalize(rows)).reconcile(schema)
        assert reconciled.dtypes.astype(str).to_dict() == {column: dtype for column, (dtype, _) in columns.items()}
        assert reconciled.to_dict(orient="records") == [{column: value for column, (_, value) in columns.items()}]


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
