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
import gzip
import json
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import pytest
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
    return AmazonAds(
        id="src-1",
        profile_id="123",
        connection=AmazonAdsConnection(client_id="x", client_secret="x", refresh_token="x", location="EU"),
    )


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


# Count metrics: whole numbers only across the 20 M legacy rows of a production warehouse (checked
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
REPORT_URL = "https://offline-report-storage.example/report.json.gz?signature=s"
REPORT_ROWS = [{"date": "2026-10-01", "campaignId": 1, "impressions": 3, "cost": 1.5}]
EVERY_STATE = {"stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]}}

# the token-paged list calls: path, vendor media type, the response key holding a page, extended data asked for
TOKEN_LISTS = {
    "products_campaigns": ("/sp/campaigns/list", "application/vnd.spCampaign.v3+json", "campaigns", True),
    "products_ad_groups": ("/sp/adGroups/list", "application/vnd.spAdGroup.v3+json", "adGroups", True),
    "products_ads": ("/sp/productAds/list", "application/vnd.spProductAd.v3+json", "productAds", True),
    "brands_campaigns": ("/sb/v4/campaigns/list", "application/vnd.sbcampaignresource.v4+json", "campaigns", True),
    "brands_ad_groups": ("/sb/v4/adGroups/list", "application/vnd.sbadgroupresource.v4+json", "adGroups", True),
    "brands_ads": ("/sb/v4/ads/list", "application/vnd.sbadresource.v4+json", "ads", False),
}
OFFSET_LISTS = {
    "display_campaigns": "/sd/campaigns/extended",
    "display_ad_groups": "/sd/adGroups/extended",
    "display_ads": "/sd/productAds/extended",
}
# every report: ad product, report type, grouping, columns
REPORTS = {
    "products_advertised_products_stats": (
        "SPONSORED_PRODUCTS",
        "spAdvertisedProduct",
        ["advertiser"],
        constants.PRODUCTS_ADVERTISED_PRODUCT_METRICS,
    ),
    "products_campaigns_stats": (
        "SPONSORED_PRODUCTS",
        "spCampaigns",
        ["campaign", "adGroup"],
        constants.PRODUCTS_CAMPAIGN_METRICS,
    ),
    "products_search_terms_stats": (
        "SPONSORED_PRODUCTS",
        "spSearchTerm",
        ["searchTerm"],
        constants.PRODUCTS_SEARCH_TERM_METRICS,
    ),
    "products_targeting_stats": (
        "SPONSORED_PRODUCTS",
        "spTargeting",
        ["targeting"],
        constants.PRODUCTS_TARGETING_METRICS,
    ),
    "products_purchased_products_stats": (
        "SPONSORED_PRODUCTS",
        "spPurchasedProduct",
        ["asin"],
        constants.PRODUCTS_PURCHASED_PRODUCT_METRICS,
    ),
    "products_gross_and_invalid_traffic_stats": (
        "SPONSORED_PRODUCTS",
        "spGrossAndInvalids",
        ["campaign"],
        constants.PRODUCTS_GROSS_AND_INVALID_TRAFFIC_METRICS,
    ),
    "display_campaigns_stats": ("SPONSORED_DISPLAY", "sdCampaigns", ["campaign"], constants.DISPLAY_CAMPAIGN_METRICS),
    "display_advertised_products_stats": (
        "SPONSORED_DISPLAY",
        "sdAdvertisedProduct",
        ["advertiser"],
        constants.DISPLAY_ADVERTISED_PRODUCT_METRICS,
    ),
    "display_purchased_products_stats": (
        "SPONSORED_DISPLAY",
        "sdPurchasedProduct",
        ["asin"],
        constants.DISPLAY_PURCHASED_PRODUCT_METRICS,
    ),
    "display_targeting_stats": ("SPONSORED_DISPLAY", "sdTargeting", ["targeting"], constants.DISPLAY_TARGETING_METRICS),
    "display_gross_and_invalid_traffic_stats": (
        "SPONSORED_DISPLAY",
        "sdGrossAndInvalids",
        ["campaign"],
        constants.DISPLAY_GROSS_AND_INVALID_TRAFFIC_METRICS,
    ),
    "display_ad_groups_stats": ("SPONSORED_DISPLAY", "sdAdGroup", ["adGroup"], constants.DISPLAY_AD_GROUP_METRICS),
    "brands_campaigns_stats": ("SPONSORED_BRANDS", "sbCampaigns", ["campaign"], constants.BRANDS_CAMPAIGN_METRICS),
    "brands_ads_stats": ("SPONSORED_BRANDS", "sbAds", ["ads"], constants.BRANDS_AD_METRICS),
    "brands_search_terms_stats": (
        "SPONSORED_BRANDS",
        "sbSearchTerm",
        ["searchTerm"],
        constants.BRANDS_SEARCH_TERM_METRICS,
    ),
    "brands_targeting_stats": ("SPONSORED_BRANDS", "sbTargeting", ["targeting"], constants.BRANDS_TARGETING_METRICS),
    "brands_purchased_products_stats": (
        "SPONSORED_BRANDS",
        "sbPurchasedProduct",
        ["purchasedAsin"],
        constants.BRANDS_PURCHASED_PRODUCT_METRICS,
    ),
    "brands_gross_and_invalid_traffic_stats": (
        "SPONSORED_BRANDS",
        "sbGrossAndInvalids",
        ["campaign"],
        constants.BRANDS_GROSS_AND_INVALID_TRAFFIC_METRICS,
    ),
    "brands_placements_stats": (
        "SPONSORED_BRANDS",
        "sbCampaignPlacement",
        ["campaignPlacement"],
        constants.BRANDS_PLACEMENT_METRICS,
    ),
    "brands_ad_groups_stats": ("SPONSORED_BRANDS", "sbAdGroup", ["adGroup"], constants.BRANDS_AD_GROUP_METRICS),
    "television_campaigns_stats": (
        "SPONSORED_TELEVISION",
        "stCampaigns",
        ["campaign"],
        constants.TELEVISION_CAMPAIGN_METRICS,
    ),
    "television_targeting_stats": (
        "SPONSORED_TELEVISION",
        "stTargeting",
        ["targeting"],
        constants.TELEVISION_TARGETING_METRICS,
    ),
}


def _source_with_api(handler: Any) -> Any:
    connection = AmazonAdsConnection(id="amz", client_id="cid", client_secret="x", refresh_token="x", location="EU")
    connection.__dict__["client"] = il.RESTClient(API_URL, transport=httpx2.MockTransport(handler))
    return AmazonAds(id="src-1", profile_id="123", connection=connection)


def _run(source: Any, key: str) -> list[dict[str, Any]]:
    asset = next(a for a in source.assets if type(a).key == key)
    return asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=DAY).window))


def _conform(key: str, rows: list[dict[str, Any]], schema: type[Schema], *, strict: bool = True) -> pd.DataFrame:
    asset = next(a for a in _source().assets if type(a).key == key)
    normalized = asset.normalizer.normalize(rows)
    return TableData.from_data(normalized).reconcile(schema, strict=strict)


def _headers(request: httpx2.Request) -> dict[str, str | None]:
    names = ("amazon-advertising-api-scope", "content-type", "accept")
    return {name: request.headers.get(name) for name in names}


def _ordered(content: bytes) -> str:
    # re-encoded with its keys in the order sent, so a comparison pins the order too
    return json.dumps(json.loads(content))


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    """Serve every report download as the gzipped JSON of ``REPORT_ROWS``, recording each call.

    Args:
        monkeypatch: Replaces ``httpx2.get`` for the test only.

    Returns:
        The URL and keyword arguments of each download, in order.
    """
    calls: list[tuple[str, dict[str, Any]]] = []

    def get(url: str, **kwargs: Any) -> httpx2.Response:
        calls.append((url, kwargs))
        content = gzip.compress(json.dumps(REPORT_ROWS).encode())
        return httpx2.Response(200, content=content, request=httpx2.Request("GET", url))

    monkeypatch.setattr(httpx2, "get", get)
    return calls


class TestEntities:
    """Campaigns and ad groups per ad product, mirroring each product's list call."""

    def test_every_entity_asset_is_pinned(self):
        entities = {type(a).key for a in _source().assets if "Entity" in a.tags}
        assert entities == {*TOKEN_LISTS, *OFFSET_LISTS, "profiles"}

    @pytest.mark.parametrize("key", sorted(TOKEN_LISTS))
    def test_a_token_list_sends_its_call_page_by_page(self, key: str):
        path, media_type, page_key, extended = TOKEN_LISTS[key]
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if "nextToken" not in json.loads(request.content):
                return httpx2.Response(200, json={page_key: [{"id": "1"}], "nextToken": "t1"})
            return httpx2.Response(200, json={page_key: [{"id": "2"}]})

        rows = _run(_source_with_api(handler), key)

        assert rows == [{"id": "1", "date": DAY}, {"id": "2", "date": DAY}]
        body = {**EVERY_STATE, **({"includeExtendedDataFields": True} if extended else {}), "maxResults": 100}
        first, second = requests
        for request in requests:
            assert (request.method, request.url.path, request.url.query) == ("POST", path, b"")
            assert _headers(request) == {
                "amazon-advertising-api-scope": "123",
                "content-type": media_type,
                "accept": media_type,
            }
        assert _ordered(first.content) == json.dumps(body)
        assert _ordered(second.content) == json.dumps({**body, "nextToken": "t1"})

    @pytest.mark.parametrize("key", sorted(OFFSET_LISTS))
    def test_an_offset_list_sends_its_call_until_an_empty_page(self, key: str):
        # the paginator advances one request in place, so each is recorded as it is sent
        calls: list[tuple[str, str, bytes, dict[str, str | None]]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append((request.method, request.url.path, request.url.query, _headers(request)))
            return httpx2.Response(200, json=[{"id": 1}] if request.url.params["startIndex"] == "0" else [])

        rows = _run(_source_with_api(handler), key)

        assert rows == [{"id": 1, "date": DAY}]
        headers = {"amazon-advertising-api-scope": "123", "content-type": None, "accept": "*/*"}
        assert calls == [
            ("GET", OFFSET_LISTS[key], b"stateFilter=enabled%2Cpaused%2Carchived&count=100&startIndex=0", headers),
            ("GET", OFFSET_LISTS[key], b"stateFilter=enabled%2Cpaused%2Carchived&count=100&startIndex=100", headers),
        ]

    @pytest.mark.parametrize(
        ("key", "schema"),
        [("products_campaigns", schemas.ProductsCampaigns), ("brands_campaigns", schemas.BrandsCampaigns)],
    )
    def test_free_form_tags_land_whole_as_json(self, key: str, schema: type[Schema]):
        page_key = TOKEN_LISTS[key][2]
        campaigns = [
            {"campaignId": "1", "tags": {"team": "a", "budget owner": "b"}},
            {"campaignId": "2", "tags": {}},
            {"campaignId": "3"},
        ]
        rows = _run(_source_with_api(lambda _: httpx2.Response(200, json={page_key: campaigns})), key)

        out = _conform(key, rows, schema)

        assert json.loads(out.loc[0, "tags"]) == {"team": "a", "budget owner": "b"}
        assert out.loc[1, "tags"] == "{}"
        assert pd.isna(out.loc[2, "tags"])
        assert not [column for column in out.columns if column.startswith("tags_")]

    def test_a_products_ad_conforms_with_its_extended_data(self):
        row = {
            "adId": "11",
            "adGroupId": "2",
            "campaignId": "1",
            "asin": "B000TEST",
            "state": "ENABLED",
            "globalStoreSetting": {"catalogSourceCountryCode": "DE"},
            "extendedData": {
                "creationDateTime": "2026-08-01T10:00:00Z",
                "servingStatus": "AD_STATUS_LIVE",
                "servingStatusDetails": [{"name": "AD_STATUS_LIVE_DETAIL", "message": "Live"}],
            },
            "date": DAY,
        }
        out = _conform("products_ads", [row], schemas.ProductsAds)
        assert (out.loc[0, "ad_id"], out.loc[0, "asin"], out.loc[0, "state"]) == ("11", "B000TEST", "ENABLED")
        assert out.loc[0, "global_store_setting_catalog_source_country_code"] == "DE"
        assert out.loc[0, "extended_data_creation_date_time"] == pd.Timestamp("2026-08-01T10:00:00", tz="UTC")
        assert json.loads(out.loc[0, "extended_data_serving_status_details"])[0]["name"] == "AD_STATUS_LIVE_DETAIL"

    def test_a_brands_ad_conforms_with_its_creative_and_landing_page(self):
        row = {
            "adId": "21",
            "adGroupId": "3",
            "campaignId": "1",
            "name": "Spring video",
            "state": "ENABLED",
            "creative": {
                "type": "VIDEO",
                "asins": ["B000A", "B000B"],
                "headline": "New in",
                "brandLogoCrop": {"top": 0, "left": 0, "width": 400, "height": 400},
                "creativeStatus": "APPROVED",
            },
            "landingPage": {"pageType": "STORE", "url": "https://www.amazon.de/stores/example"},
            "extendedData": {"creationDate": 1785179053888, "servingStatus": "AD_STATUS_LIVE"},
            "date": DAY,
        }
        out = _conform("brands_ads", [row], schemas.BrandsAds)
        assert (out.loc[0, "name"], out.loc[0, "creative_type"], out.loc[0, "creative_headline"]) == (
            "Spring video",
            "VIDEO",
            "New in",
        )
        assert json.loads(out.loc[0, "creative_asins"]) == ["B000A", "B000B"]
        assert json.loads(out.loc[0, "creative_brand_logo_crop"])["width"] == 400
        assert out.loc[0, "creative_creative_status"] == "APPROVED"
        assert out.loc[0, "landing_page_page_type"] == "STORE"
        assert out.loc[0, "extended_data_creation_date"] == pd.Timestamp("2026-07-27T19:04:13.888", tz="UTC")

    def test_a_display_ad_conforms_with_exact_ids_and_epoch_times(self):
        row = {
            "adId": 287654321098765433,
            "adGroupId": 4,
            "campaignId": 5,
            "asin": "B000C",
            "state": "enabled",
            "servingStatus": "AD_STATUS_LIVE",
            "creationDate": 1785179053888,
            "date": DAY,
        }
        out = _conform("display_ads", [row], schemas.DisplayAds)
        assert (out.loc[0, "ad_id"], out.loc[0, "ad_group_id"], out.loc[0, "campaign_id"]) == (
            "287654321098765433",
            "4",
            "5",
        )
        assert out.loc[0, "creation_date"] == pd.Timestamp("2026-07-27T19:04:13.888", tz="UTC")

    def test_display_entities_keep_their_date_reading_normalizer_through_the_roundtrip(self):
        for key in ("display_campaigns", "display_ad_groups"):
            src = _source()
            asset = next(a for a in src.assets if type(a).key == key)
            spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
            child: Any = next(a for a in DAGSpec(**spec_json).reconstruct().operations if type(a).key == key)
            assert type(child.normalizer).__name__ == "SponsoredDisplayEntityNormalizer", key

    def test_campaigns_keep_their_tags_whole_through_the_roundtrip(self):
        for key in ("products_campaigns", "brands_campaigns"):
            src = _source()
            asset = next(a for a in src.assets if type(a).key == key)
            spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
            child: Any = next(a for a in DAGSpec(**spec_json).reconstruct().operations if type(a).key == key)
            out = child.normalizer.normalize([{"campaignId": "1", "tags": {"team": "a"}, "date": DAY}])
            assert json.loads(out.loc[0, "tags"]) == {"team": "a"}, key

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
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(
                200,
                json=[
                    {"profileId": 123, "countryCode": "DE", "dailyBudget": 10.0, "accountInfo": {"subType": "KDP"}},
                    {"profileId": 999, "countryCode": "FR"},
                ],
            )

        rows = _run(_source_with_api(handler), "profiles")
        assert rows == [
            {
                "profileId": 123,
                "countryCode": "DE",
                "dailyBudget": 10.0,
                "accountInfo": {"subType": "KDP"},
                "date": DAY,
            }
        ]
        # the profile listing is the one call Amazon does not scope to a profile
        [request] = requests
        assert (request.method, request.url.path, request.url.query) == ("GET", "/v2/profiles", b"")
        assert _headers(request) == {"amazon-advertising-api-scope": None, "content-type": None, "accept": "*/*"}
        # /v2/profiles sends profileId as a number, which the default RECONCILE cast turns into the text id
        out = _conform("profiles", rows, schemas.Profiles, strict=False)
        assert out.loc[0, "profile_id"] == "123"
        assert out.loc[0, "daily_budget"] == 10.0 and out.loc[0, "account_info_sub_type"] == "KDP"
        assert pd.isna(out.loc[0, "account_info_name"])

    def test_a_profile_the_credentials_no_longer_see_fails(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(200, json=[{"profileId": 999, "countryCode": "FR"}])

        with pytest.raises(RuntimeError, match="does not list profile 123"):
            _run(_source_with_api(handler), "profiles")


class TestReports:
    def test_every_report_asset_is_pinned(self):
        reports = {type(a).key for a in _source().assets if "Report" in a.tags}
        assert reports == set(REPORTS)

    @pytest.mark.parametrize("key", sorted(REPORTS))
    def test_runs_one_report_job_for_the_day_and_returns_its_rows_untouched(
        self, key: str, downloads: list[tuple[str, dict[str, Any]]]
    ):
        ad_product, report_type_id, group_by, columns = REPORTS[key]
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.method == "POST":
                return httpx2.Response(200, json={"reportId": "r-1", "status": "PENDING"})
            return httpx2.Response(200, json={"reportId": "r-1", "status": "COMPLETED", "url": REPORT_URL})

        rows = _run(_source_with_api(handler), key)

        assert rows == REPORT_ROWS
        report_headers = {
            "amazon-advertising-api-scope": "123",
            "content-type": "application/vnd.createasyncreportrequest.v3+json",
            "accept": "*/*",
        }
        create, status = requests
        assert (create.method, create.url.path, create.url.query) == ("POST", "/reporting/reports", b"")
        assert _headers(create) == report_headers
        assert _ordered(create.content) == json.dumps(
            {
                "name": f"{report_type_id} | 2026-10-01 - 2026-10-01",
                "startDate": "2026-10-01",
                "endDate": "2026-10-01",
                "configuration": {
                    "reportTypeId": report_type_id,
                    "adProduct": ad_product,
                    "groupBy": group_by,
                    "columns": columns,
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            }
        )
        assert (status.method, status.url.path, status.url.query) == ("GET", "/reporting/reports/r-1", b"")
        assert _headers(status) == report_headers
        assert downloads == [(REPORT_URL, {"timeout": 60.0})]


class TestTelevisionReports:
    def test_both_television_reports_declare_a_schema(self):
        src = _source()
        for key in ("television_campaigns_stats", "television_targeting_stats"):
            asset = next(a for a in src.assets if type(a).key == key)
            assert asset.schema is not None, key
