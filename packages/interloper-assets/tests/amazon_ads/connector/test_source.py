"""Regression tests for the AmazonAds source configuration.

Prod incident (run d0aebe6d, 2026-06-10): the source-level
``DataFrameNormalizer`` never reached asset instances, so raw camelCase
report rows hit schema validation and every report asset failed with
"Field required" errors. Two distinct defects were involved:

1. ``@il.source(normalizer=...)`` field args were silently dropped on
   class-based sources (``build_class`` setattr on a built
   pydantic class).
2. The host→child DAG-spec round-trip dumped the normalizer as a bare
   dict, degrading ``DataFrameNormalizer`` to the base ``Normalizer``.

These tests pin the whole chain: live instance → spec JSON → reconstructed
child asset → normalize+validate a camelCase report row.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pandas as pd
from interloper.asset import Asset
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper.schema import Schema
from interloper_pandas import DataFrameNormalizer

from interloper_assets.amazon_ads import constants
from interloper_assets.amazon_ads.connection import AmazonAdsConnection
from interloper_assets.amazon_ads.connector import schemas
from interloper_assets.amazon_ads.connector.source import AmazonAds


def _source() -> Any:
    return AmazonAds(id="src-1", profile_id="123")  # ty: ignore[missing-argument]


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)
        assert src.normalizer.snake_case_digits is True
        assert src.normalizer.flatten_max_level == 1

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "products_campaigns_stats")

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "products_campaigns_stats")
        assert isinstance(child_asset, Asset)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 1
        assert normalizer.column_overrides == {
            "eCPAddToCart": "ecp_add_to_cart",
            "eCPBrandSearch": "ecp_brand_search",
        }

    def test_camelcase_report_row_conforms_after_roundtrip(self):
        """A raw API-shaped row must normalize and validate against the schema."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "products_campaigns_stats")
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "products_campaigns_stats")
        assert isinstance(child_asset, Asset)

        # One row with every requested report column, as Amazon returns them.
        # All schema fields are required-nullable, so None is valid everywhere.
        row: dict[str, object] = dict.fromkeys(constants.PRODUCTS_CAMPAIGN_METRICS)
        row["date"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        TableData.from_data(normalized).reconcile(schemas.ProductsCampaignsStats)  # must not raise


# Count metrics: whole numbers only across the 20 M legacy rows of the Swarovski warehouse (checked
# 2026-09-16), so the schemas type them as ``int``. Rates and amounts keep ``float``.
COUNT_FIELDS = frozenset(
    {
        "impressions",
        "clicks",
        "purchases",
        "purchases_clicks",
        "units_sold",
        "units_sold_clicks",
        "purchases_1d",
        "purchases_7d",
        "purchases_14d",
        "purchases_30d",
        "units_sold_clicks_1d",
        "units_sold_clicks_7d",
        "units_sold_clicks_14d",
        "units_sold_clicks_30d",
        "units_sold_same_sku_1d",
        "units_sold_same_sku_7d",
        "units_sold_same_sku_14d",
        "units_sold_same_sku_30d",
        "purchases_same_sku_1d",
        "purchases_same_sku_7d",
        "purchases_same_sku_14d",
        "purchases_same_sku_30d",
        "new_to_brand_purchases",
        "new_to_brand_units_sold",
        "detail_page_views",
        "add_to_cart",
        "branded_searches",
        "viewable_impressions",
        "video_complete_views",
        "video_unmutes",
        "cumulative_reach",
        "gross_impressions",
        "invalid_impressions",
        "gross_click_throughs",
        "invalid_click_throughs",
    }
)
FLOAT_SUFFIXES = (
    "_rate",
    "_percentage",
    "cost",
    "sales",
    "spend",
    "bid",
    "amount",
    "roas_clicks_7d",
    "roas_clicks_14d",
)


def _stats_schemas() -> list[type[Schema]]:
    import inspect

    return [
        cls
        for name, cls in inspect.getmembers(schemas, inspect.isclass)
        if issubclass(cls, Schema) and cls is not Schema and name.endswith("Stats")
    ]


class TestCountMetricTypes:
    """Count metrics are integers; rates and amounts stay floats."""

    def test_count_metrics_are_int_everywhere(self):
        wrong = [
            f"{cls.__name__}.{name}"
            for cls in _stats_schemas()
            for name, info in cls.model_fields.items()
            if name in COUNT_FIELDS and info.annotation != (int | None)
        ]
        assert not wrong, wrong

    def test_rates_and_amounts_stay_float(self):
        wrong = [
            f"{cls.__name__}.{name}"
            for cls in _stats_schemas()
            for name, info in cls.model_fields.items()
            if name.endswith(FLOAT_SUFFIXES) and info.annotation == (int | None)
        ]
        assert not wrong, wrong

    def test_budget_rule_name_is_a_string(self):
        field = schemas.ProductsCampaignsStats.model_fields["campaign_applicable_budget_rule_name"]
        assert field.annotation == (str | None)

    def test_float_valued_counts_reconcile_to_int(self):
        """Amazon may serialise a count as ``3.0``; conform must land it as an integer column."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "products_campaigns_stats")
        row: dict[str, object] = dict.fromkeys(constants.PRODUCTS_CAMPAIGN_METRICS)
        row.update({"date": "2026-06-10", "impressions": 3.0, "clicks": 2, "cost": 1.5})
        normalizer = asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(pd.DataFrame([row]))
        out = TableData.from_data(normalized).reconcile(schemas.ProductsCampaignsStats)
        assert pd.api.types.is_integer_dtype(out["impressions"]) and int(out.loc[0, "impressions"]) == 3
        assert pd.api.types.is_float_dtype(out["cost"])


DAY = dt.date(2026, 10, 1)
API_URL = "https://advertising-api-eu.amazon.com"


def _source_with_api(handler: Any) -> Any:
    connection = AmazonAdsConnection(id="amz", client_id="cid", client_secret="x", refresh_token="x", location="EU")
    connection.__dict__["client"] = il.RESTClient(API_URL, transport=httpx2.MockTransport(handler))
    return AmazonAds(id="src-1", profile_id="123", connection=connection)


def _run(source: Any, key: str) -> list[dict[str, Any]]:
    asset = next(a for a in source.assets if type(a).key == key)
    return asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=DAY).window))


def _conform(key: str, rows: list[dict[str, Any]], schema: type[Schema]) -> pd.DataFrame:
    asset = next(a for a in _source().assets if type(a).key == key)
    normalized = asset.normalizer.normalize(rows)
    return TableData.from_data(normalized).reconcile(schema, strict=True)


class TestEntities:
    """Campaigns and ad groups per ad product, mirroring each product's list call."""

    def test_products_campaigns_page_through_the_next_token(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            body = json.loads(request.content)
            if body.get("nextToken") is None:
                page = {"campaigns": [{"campaignId": "1", "tags": {"team": "a"}}], "nextToken": "t1"}
            else:
                page = {"campaigns": [{"campaignId": "2"}]}
            return httpx2.Response(200, json=page)

        rows = _run(_source_with_api(handler), "products_campaigns")
        assert [row["campaignId"] for row in rows] == ["1", "2"]
        assert all(row["date"] == DAY for row in rows)
        assert json.loads(rows[0]["tags"]) == {"team": "a"}
        first, second = requests
        assert (first.method, first.url.path) == ("POST", "/sp/campaigns/list")
        assert first.headers["Amazon-Advertising-API-Scope"] == "123"
        assert first.headers["Content-Type"] == "application/vnd.spCampaign.v3+json"
        assert first.headers["Accept"] == "application/vnd.spCampaign.v3+json"
        assert json.loads(first.content) == {
            "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
            "includeExtendedDataFields": True,
            "maxResults": 100,
        }
        assert json.loads(second.content)["nextToken"] == "t1"

    def test_brands_ad_groups_use_the_v4_resource(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"adGroups": [{"adGroupId": "7", "campaignId": "1"}]})

        rows = _run(_source_with_api(handler), "brands_ad_groups")
        assert [row["adGroupId"] for row in rows] == ["7"]
        [request] = requests
        assert request.url.path == "/sb/v4/adGroups/list"
        assert request.headers["Content-Type"] == "application/vnd.sbadgroupresource.v4+json"

    def test_display_campaigns_page_by_offset_until_empty(self):
        calls: list[tuple[str, str, dict[str, str]]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            params = dict(request.url.params)
            calls.append((request.url.path, request.headers["Amazon-Advertising-API-Scope"], params))
            return httpx2.Response(200, json=[{"campaignId": 5, "name": "SD"}] if params["startIndex"] == "0" else [])

        rows = _run(_source_with_api(handler), "display_campaigns")
        assert [row["campaignId"] for row in rows] == [5]
        assert calls == [
            (
                "/sd/campaigns/extended",
                "123",
                {"startIndex": "0", "count": "100", "stateFilter": "enabled,paused,archived"},
            ),
            (
                "/sd/campaigns/extended",
                "123",
                {"startIndex": "100", "count": "100", "stateFilter": "enabled,paused,archived"},
            ),
        ]

    def test_every_entity_asset_calls_its_own_list(self):
        calls = {
            "products_campaigns": ("POST", "/sp/campaigns/list", "application/vnd.spCampaign.v3+json", "campaigns"),
            "products_ad_groups": ("POST", "/sp/adGroups/list", "application/vnd.spAdGroup.v3+json", "adGroups"),
            "brands_campaigns": (
                "POST",
                "/sb/v4/campaigns/list",
                "application/vnd.sbcampaignresource.v4+json",
                "campaigns",
            ),
            "brands_ad_groups": (
                "POST",
                "/sb/v4/adGroups/list",
                "application/vnd.sbadgroupresource.v4+json",
                "adGroups",
            ),
            "display_campaigns": ("GET", "/sd/campaigns/extended", None, None),
            "display_ad_groups": ("GET", "/sd/adGroups/extended", None, None),
        }
        for key, (method, path, media_type, page_key) in calls.items():
            seen: list[httpx2.Request] = []

            def handler(request: httpx2.Request, seen=seen, page_key=page_key) -> httpx2.Response:
                seen.append(request)
                if page_key is None:
                    return httpx2.Response(200, json=[{"id": 1}] if request.url.params["startIndex"] == "0" else [])
                return httpx2.Response(200, json={page_key: [{"id": "1"}]})

            rows = _run(_source_with_api(handler), key)
            assert rows == [{"id": rows[0]["id"], "date": DAY}], key
            assert (seen[0].method, seen[0].url.path) == (method, path), key
            if media_type is not None:
                assert seen[0].headers["Content-Type"] == media_type, key

    def test_display_entities_keep_their_date_reading_normalizer_through_the_roundtrip(self):
        for key in ("display_campaigns", "display_ad_groups"):
            src = _source()
            asset = next(a for a in src.assets if type(a).key == key)
            spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
            child: Any = next(a for a in DAGSpec(**spec_json).reconstruct().operations if type(a).key == key)
            assert type(child.normalizer).__name__ == "SponsoredDisplayEntityNormalizer", key

    def test_products_lifecycle_times_conform_whatever_their_precision(self):
        # Sponsored Products sends milliseconds only when they are not zero
        rows = [
            {"campaignId": "1", "extendedData": {"creationDateTime": "2022-02-21T20:00:54Z"}, "date": DAY},
            {"campaignId": "2", "extendedData": {"creationDateTime": "2022-02-21T20:00:54.497Z"}, "date": DAY},
        ]
        out = _conform("products_campaigns", rows, schemas.ProductsCampaigns)
        assert out["extended_data_creation_date_time"].tolist() == [
            pd.Timestamp("2022-02-21T20:00:54", tz="UTC"),
            pd.Timestamp("2022-02-21T20:00:54.497", tz="UTC"),
        ]

    def test_a_products_campaign_conforms_with_its_nested_budget(self):
        row = {
            "campaignId": "1",
            "name": "SP",
            "state": "ENABLED",
            "targetingType": "AUTO",
            "startDate": "2026-09-01",
            "endDate": None,
            "budget": {"budget": 50.0, "budgetType": "DAILY", "effectiveBudget": 45.0},
            "dynamicBidding": {
                "strategy": "LEGACY_FOR_SALES",
                "placementBidding": [{"placement": "X", "percentage": 10}],
            },
            "extendedData": {"creationDateTime": "2026-08-01T10:00:00Z", "servingStatus": "CAMPAIGN_STATUS_ENABLED"},
            "date": DAY,
        }
        out = _conform("products_campaigns", [row], schemas.ProductsCampaigns)
        assert out.loc[0, "budget_budget"] == 50.0 and out.loc[0, "budget_budget_type"] == "DAILY"
        assert out.loc[0, "start_date"] == dt.date(2026, 9, 1)
        assert json.loads(out.loc[0, "dynamic_bidding_placement_bidding"]) == [{"placement": "X", "percentage": 10}]
        assert out.loc[0, "extended_data_serving_status"] == "CAMPAIGN_STATUS_ENABLED"

    def test_a_display_campaign_conforms_with_its_compact_dates(self):
        row = {
            "campaignId": 5,
            "name": "SD",
            "state": "enabled",
            "startDate": "20261001",
            "creationDate": 1785179053888,
            "lastUpdatedDate": None,
            "date": DAY,
        }
        out = _conform("display_campaigns", [row], schemas.DisplayCampaigns)
        assert out.loc[0, "start_date"] == dt.date(2026, 10, 1)
        assert int(out.loc[0, "campaign_id"]) == 5
        assert out.loc[0, "creation_date"] == pd.Timestamp("2026-07-27T19:04:13.888", tz="UTC")
        assert pd.isna(out.loc[0, "last_updated_date"])

    def test_display_ids_become_exact_strings(self):
        # an 18-digit id is beyond what a float holds exactly, and a missing portfolio would make the column float
        rows = [
            {"campaignId": 287654321098765433, "portfolioId": None, "date": DAY},
            {"campaignId": 5, "portfolioId": 123456789012345, "date": DAY},
        ]
        out = _conform("display_campaigns", rows, schemas.DisplayCampaigns)
        assert out["campaign_id"].tolist() == ["287654321098765433", "5"]
        assert pd.isna(out.loc[0, "portfolio_id"]) and out.loc[1, "portfolio_id"] == "123456789012345"
        groups = _conform(
            "display_ad_groups",
            [{"adGroupId": 287654321098765433, "campaignId": 5, "date": DAY}],
            schemas.DisplayAdGroups,
        )
        assert (groups.loc[0, "ad_group_id"], groups.loc[0, "campaign_id"]) == ("287654321098765433", "5")

    def test_epoch_milliseconds_become_instants_on_brands_and_display(self):
        stamped = {
            "brands_campaigns": (
                schemas.BrandsCampaigns,
                {"extendedData": {"creationDate": 1785179053888, "lastUpdateDate": 1785265453888}},
                ("extended_data_creation_date", "extended_data_last_update_date"),
            ),
            "brands_ad_groups": (
                schemas.BrandsAdGroups,
                {"extendedData": {"creationDate": 1785179053888, "lastUpdateDate": 1785265453888}},
                ("extended_data_creation_date", "extended_data_last_update_date"),
            ),
            "display_ad_groups": (
                schemas.DisplayAdGroups,
                {"creationDate": 1785179053888, "lastUpdatedDate": 1785265453888},
                ("creation_date", "last_updated_date"),
            ),
        }
        for key, (schema, row, (created, updated)) in stamped.items():
            out = _conform(key, [{"campaignId": "1", **row, "date": DAY}], schema)
            assert out.loc[0, created] == pd.Timestamp("2026-07-27T19:04:13.888", tz="UTC"), key
            assert out.loc[0, updated] == pd.Timestamp("2026-07-28T19:04:13.888", tz="UTC"), key

    def test_every_entity_row_conforms_with_its_schema(self):
        cases = {
            "brands_campaigns": (schemas.BrandsCampaigns, {"campaignId": "1", "budget": 20.0, "budgetType": "DAILY"}),
            "products_ad_groups": (schemas.ProductsAdGroups, {"adGroupId": "2", "campaignId": "1", "defaultBid": 0.5}),
            "brands_ad_groups": (schemas.BrandsAdGroups, {"adGroupId": "3", "campaignId": "1", "state": "ENABLED"}),
            "display_ad_groups": (schemas.DisplayAdGroups, {"adGroupId": 4, "campaignId": 5, "tactic": "T00020"}),
        }
        for key, (schema, row) in cases.items():
            _conform(key, [{**row, "date": DAY}], schema)


class TestProfiles:
    def test_profiles_keep_only_the_sources_own_profile(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.url.path == "/v2/profiles"
            return httpx2.Response(
                200,
                json=[
                    {"profileId": 123, "countryCode": "DE", "dailyBudget": 10.0, "accountInfo": {"subType": "KDP"}},
                    {"profileId": 999, "countryCode": "FR"},
                ],
            )

        rows = _run(_source_with_api(handler), "profiles")
        assert [row["profileId"] for row in rows] == [123]
        out = _conform("profiles", rows, schemas.Profiles)
        assert out.loc[0, "daily_budget"] == 10.0 and out.loc[0, "account_info_sub_type"] == "KDP"
        assert pd.isna(out.loc[0, "account_info_name"])


class TestTelevisionReports:
    def test_both_television_reports_declare_a_schema(self):
        src = _source()
        for key in ("television_campaigns_stats", "television_targeting_stats"):
            asset = next(a for a in src.assets if type(a).key == key)
            assert asset.schema is not None, key
