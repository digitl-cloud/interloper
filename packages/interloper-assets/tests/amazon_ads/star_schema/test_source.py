"""Tests for the Amazon Ads star schema: views over every profile's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.runner import ExecutionStatus

from interloper_assets.amazon_ads.connection import AmazonAdsConnection
from interloper_assets.amazon_ads.connector.source import AmazonAds
from interloper_assets.amazon_ads.star_schema import schemas
from interloper_assets.amazon_ads.star_schema.source import AmazonAdsStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)
CREATED_SQL, UPDATED_SQL = "TIMESTAMPTZ '2026-07-27 19:04:13.888+00'", "TIMESTAMPTZ '2026-07-28 19:04:13.888+00'"
CREATED = dt.datetime(2026, 7, 27, 19, 4, 13, 888000, tzinfo=dt.timezone.utc)
UPDATED = dt.datetime(2026, 7, 28, 19, 4, 13, 888000, tzinfo=dt.timezone.utc)
CAMPAIGN_REPORTS = [
    "amazon_ads.products_campaigns_stats",
    "amazon_ads.brands_campaigns_stats",
    "amazon_ads.display_campaigns_stats",
]
AD_GROUP_REPORTS = [
    "amazon_ads.products_campaigns_stats",
    "amazon_ads.brands_ad_groups_stats",
    "amazon_ads.display_ad_groups_stats",
]
FACT_CORE = [
    "date",
    "account_id",
    "campaign_id",
    "impressions",
    "clicks",
    "cost",
    "currency",
    "conversions",
    "conversion_value",
    "purchases",
    "revenue",
]
ATTRIBUTE_CORE = ["budget", "budget_type", "start_date", "end_date", "created_at", "updated_at", "as_of_date"]


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _profile(profile_id: str, warehouse) -> il.Source:
    connection = AmazonAdsConnection(id="amz", client_id="x", client_secret="x", refresh_token="x", location="EU")
    return AmazonAds(profile_id=profile_id, connection=connection, destinations=[warehouse], enabled=False)


def _seed(warehouse, profile_id: str, *, entity: bool, campaign_entities: bool = False) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS amazon_ads")
    collected = "TRUE" if campaign_entities else "FALSE"
    client.execute(
        f"CREATE TABLE amazon_ads.products_campaigns__{profile_id} AS SELECT "
        f"DATE '2026-09-30' AS date, '11{profile_id}' AS campaign_id, 'SP from entity' AS name, 'ENABLED' AS state, "
        "55.0 AS budget_budget, 'DAILY' AS budget_budget_type, DATE '2026-09-01' AS start_date, "
        "CAST(NULL AS DATE) AS end_date, TIMESTAMP '2026-08-01 10:00:00' AS extended_data_creation_date_time, "
        f"TIMESTAMP '2026-08-02 10:00:00' AS extended_data_last_update_date_time WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.brands_campaigns__{profile_id} AS SELECT "
        f"DATE '2026-09-30' AS date, '22{profile_id}' AS campaign_id, 'SB from entity' AS name, 'PAUSED' AS state, "
        "25.0 AS budget, 'LIFETIME' AS budget_type, DATE '2026-09-15' AS start_date, DATE '2026-12-31' AS end_date, "
        f"{CREATED_SQL} AS extended_data_creation_date, {UPDATED_SQL} AS extended_data_last_update_date "
        f"WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.display_campaigns__{profile_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 33{profile_id} AS campaign_id, 'SD from entity' AS name, 'enabled' AS state, "
        "10.0 AS budget, 'daily' AS budget_type, DATE '2026-09-20' AS start_date, CAST(NULL AS DATE) AS end_date, "
        f"{CREATED_SQL} AS creation_date, {UPDATED_SQL} AS last_updated_date WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.products_ad_groups__{profile_id} AS SELECT "
        f"DATE '2026-09-30' AS date, '1' AS ad_group_id, '11{profile_id}' AS campaign_id, "
        "'SP group from entity' AS name, 'ENABLED' AS state, "
        "TIMESTAMP '2026-08-01 10:00:00' AS extended_data_creation_date_time, "
        f"TIMESTAMP '2026-08-02 10:00:00' AS extended_data_last_update_date_time WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.brands_ad_groups__{profile_id} AS SELECT "
        f"DATE '2026-09-30' AS date, '3' AS ad_group_id, '22{profile_id}' AS campaign_id, "
        f"'SB group from entity' AS name, 'PAUSED' AS state, {CREATED_SQL} AS extended_data_creation_date, "
        f"{UPDATED_SQL} AS extended_data_last_update_date WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.display_ad_groups__{profile_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 4 AS ad_group_id, 33{profile_id} AS campaign_id, 'SD group from entity' AS name, "
        f"'enabled' AS state, {CREATED_SQL} AS creation_date, {UPDATED_SQL} AS last_updated_date WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.products_campaigns_stats__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 11{profile_id}, 'SP campaign', 'ENABLED', 50.0, 'DAILY', 'EUR', "
        "100, 10, 12.5, 2, 40.0, 1, 'SP group one'), "
        f"(DATE '2026-10-01', 11{profile_id}, 'SP campaign renamed', CAST(NULL AS VARCHAR), 50.0, 'DAILY', 'EUR', "
        "50, 5, 2.5, 1, 10.0, 2, 'SP group two')"
        ") AS t(date, campaign_id, campaign_name, campaign_status, campaign_budget_amount, campaign_budget_type, "
        "campaign_budget_currency_code, impressions, clicks, spend, purchases_14d, sales_14d, ad_group_id, "
        "ad_group_name)"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.brands_campaigns_stats__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 22{profile_id}, 'SB campaign', 'PAUSED', 20.0, 'DAILY', 'EUR', 30, 3, 4.0, 1, 15.0)"
        ") AS t(date, campaign_id, campaign_name, campaign_status, campaign_budget_amount, campaign_budget_type, "
        "campaign_budget_currency_code, impressions, clicks, cost, purchases, sales)"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.display_campaigns_stats__{profile_id} (date DATE, campaign_id BIGINT, "
        "campaign_name VARCHAR, campaign_status VARCHAR, campaign_budget_amount DOUBLE, "
        "campaign_budget_currency_code VARCHAR, impressions BIGINT, clicks BIGINT, cost DOUBLE, "
        "purchases BIGINT, sales DOUBLE)"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.brands_ad_groups_stats__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 22{profile_id}, 'SB campaign', 3, 'SB group')"
        ") AS t(date, campaign_id, campaign_name, ad_group_id, ad_group_name)"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.display_ad_groups_stats__{profile_id} (date DATE, campaign_id BIGINT, "
        "campaign_name VARCHAR, ad_group_id BIGINT, ad_group_name VARCHAR)"
    )
    collected = "TRUE" if entity else "FALSE"
    client.execute(
        f"CREATE TABLE amazon_ads.profiles__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-09-30', {profile_id}, 'DE', 'EUR', 'Europe/Berlin', 'A1PA6795UKMFR9', 'acc', 'vendor', "
        "'Acme DE', TRUE), "
        "(DATE '2026-09-30', 999, 'FR', 'EUR', 'Europe/Paris', 'A13V1IB3VIYZZH', 'acc', 'vendor', 'Someone else', TRUE)"
        ") AS t(date, profile_id, country_code, currency_code, timezone, account_info_marketplace_string_id, "
        f"account_info_id, account_info_type, account_info_name, account_info_valid_payment_method) WHERE {collected}"
    )


def _materialize(warehouse, *profiles: il.Source) -> il.Source:
    star = AmazonAdsStarSchema(destinations=[warehouse])
    il.DAG(*profiles, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "amazon_ads", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    def test_raw_upstreams_fan_in_every_profile_and_are_optional(self):
        fact = AmazonAdsStarSchema.fact_campaign_performance.relations["campaigns_stats"]
        assert (fact.key, fact.many, fact.optional) == (CAMPAIGN_REPORTS, True, True)
        assert AmazonAdsStarSchema.dim_campaigns.relations["campaigns_stats"].key == CAMPAIGN_REPORTS
        assert AmazonAdsStarSchema.dim_campaigns.relations["campaigns"].key == [
            "amazon_ads.products_campaigns",
            "amazon_ads.brands_campaigns",
            "amazon_ads.display_campaigns",
        ]
        assert AmazonAdsStarSchema.dim_ad_groups.relations["ad_groups"].key == [
            "amazon_ads.products_ad_groups",
            "amazon_ads.brands_ad_groups",
            "amazon_ads.display_ad_groups",
        ]
        ad_groups = AmazonAdsStarSchema.dim_ad_groups.relations["ad_groups_stats"]
        assert (ad_groups.key, ad_groups.many, ad_groups.optional) == (AD_GROUP_REPORTS, True, True)
        profiles = AmazonAdsStarSchema.dim_accounts.relations["profiles"]
        assert (profiles.key, profiles.many, profiles.optional) == ("amazon_ads.profiles", True, True)

    def test_views_carry_the_core_columns_first(self):
        assert list(schemas.FactCampaignPerformance.model_fields) == [*FACT_CORE, "campaign_type"]
        assert list(schemas.DimAccounts.model_fields)[:5] == [
            "account_id",
            "account_name",
            "currency",
            "timezone",
            "as_of_date",
        ]
        assert list(schemas.DimCampaigns.model_fields) == [
            "campaign_id",
            "account_id",
            "campaign_name",
            "status",
            "objective",
            "campaign_type",
            *ATTRIBUTE_CORE,
        ]
        assert list(schemas.DimAdGroups.model_fields) == [
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_group_name",
            "status",
            *ATTRIBUTE_CORE,
        ]

    def test_views_are_unpartitioned_tagged_and_grouped_under_amazon_ads(self):
        for asset in AmazonAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert AmazonAdsStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert AmazonAdsStarSchema.dim_ad_groups.tags == ["Dimension"]
        assert AmazonAdsStarSchema.group == "amazon_ads"
        assert AmazonAdsStarSchema().dataset == "amazon_ads"


class TestOnDuckDB:
    def test_the_fact_unions_every_profile_and_ad_product_at_campaign_grain(self, warehouse):
        first, second = _profile("1", warehouse), _profile("2", warehouse)
        _seed(warehouse, "1", entity=True)
        _seed(warehouse, "2", entity=False)
        _materialize(warehouse, first, second)
        rows = _rows(warehouse, "fact_campaign_performance")
        keys = [(row["date"], row["account_id"], row["campaign_type"], row["campaign_id"]) for row in rows]
        assert len(keys) == len(set(keys)) == 4
        assert {(row["account_id"], row["campaign_type"]) for row in rows} == {
            ("1", "sponsored_products"),
            ("1", "sponsored_brands"),
            ("2", "sponsored_products"),
            ("2", "sponsored_brands"),
        }
        assert "campaign_name" not in rows[0]
        [products] = [row for row in rows if (row["account_id"], row["campaign_type"]) == ("1", "sponsored_products")]
        assert (products["date"], products["campaign_id"], products["currency"]) == (DAY, "111", "EUR")
        assert (products["impressions"], products["clicks"], products["cost"]) == (150, 15, 15.0)
        assert (products["conversions"], products["conversion_value"]) == (3, 50.0)
        assert (products["purchases"], products["revenue"]) == (3, 50.0)
        [brands] = [row for row in rows if (row["account_id"], row["campaign_type"]) == ("1", "sponsored_brands")]
        assert (brands["campaign_id"], brands["cost"], brands["purchases"], brands["revenue"]) == ("221", 4.0, 1, 15.0)

    def test_campaigns_come_from_the_reports_then_the_facts(self, warehouse):
        first = _profile("1", warehouse)
        _seed(warehouse, "1", entity=False)
        _materialize(warehouse, first)
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["111"]["campaign_type"] == "sponsored_products"
        assert (rows["221"]["status"], rows["221"]["budget"], rows["221"]["budget_type"]) == ("PAUSED", 20.0, "DAILY")
        assert rows["221"]["account_id"] == "1"
        assert rows["221"]["start_date"] is None and rows["221"]["created_at"] is None

    def test_tied_report_rows_collapse_deterministically(self, warehouse):
        first = _profile("1", warehouse)
        _seed(warehouse, "1", entity=False)
        _materialize(warehouse, first)
        before = _rows(warehouse, "dim_campaigns")
        [products] = [row for row in before if row["campaign_id"] == "111"]
        assert (products["campaign_name"], products["status"]) == ("SP campaign renamed", "ENABLED")
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_campaigns") == before

    def test_ad_groups_come_from_every_ad_product_report(self, warehouse):
        first = _profile("1", warehouse)
        _seed(warehouse, "1", entity=False)
        _materialize(warehouse, first)
        rows = {row["ad_group_id"]: row for row in _rows(warehouse, "dim_ad_groups")}
        assert set(rows) == {"1", "2", "3"}
        assert (rows["1"]["campaign_id"], rows["1"]["ad_group_name"]) == ("111", "SP group one")
        assert (rows["3"]["campaign_id"], rows["3"]["ad_group_name"]) == ("221", "SB group")
        assert rows["3"]["account_id"] == "1" and rows["3"]["status"] is None
        assert rows["3"]["as_of_date"] == DAY

    def test_campaigns_take_their_entity_row_with_the_schedule(self, warehouse):
        first = _profile("1", warehouse)
        _seed(warehouse, "1", entity=False, campaign_entities=True)
        _materialize(warehouse, first)
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        products = rows["111"]
        assert (products["campaign_name"], products["status"], products["campaign_type"]) == (
            "SP from entity",
            "ENABLED",
            "sponsored_products",
        )
        assert (products["budget"], products["budget_type"]) == (55.0, "DAILY")
        assert (products["start_date"], products["end_date"]) == (dt.date(2026, 9, 1), None)
        assert products["created_at"] == dt.datetime(2026, 8, 1, 10, 0, tzinfo=dt.timezone.utc)
        assert products["as_of_date"] == dt.date(2026, 9, 30)
        brands = rows["221"]
        assert (brands["budget_type"], brands["start_date"], brands["end_date"]) == (
            "LIFETIME",
            dt.date(2026, 9, 15),
            dt.date(2026, 12, 31),
        )
        assert (brands["created_at"], brands["updated_at"]) == (CREATED, UPDATED)
        display = rows["331"]
        assert (display["account_id"], display["campaign_type"], display["start_date"]) == (
            "1",
            "sponsored_display",
            dt.date(2026, 9, 20),
        )
        assert (display["created_at"], display["updated_at"]) == (CREATED, UPDATED)

    def test_ad_groups_take_their_entity_row(self, warehouse):
        first = _profile("1", warehouse)
        _seed(warehouse, "1", entity=False, campaign_entities=True)
        _materialize(warehouse, first)
        rows = {row["ad_group_id"]: row for row in _rows(warehouse, "dim_ad_groups")}
        assert (rows["1"]["ad_group_name"], rows["1"]["status"]) == ("SP group from entity", "ENABLED")
        assert rows["1"]["updated_at"] == dt.datetime(2026, 8, 2, 10, 0, tzinfo=dt.timezone.utc)
        assert (rows["3"]["status"], rows["3"]["as_of_date"]) == ("PAUSED", dt.date(2026, 9, 30))
        assert (rows["4"]["campaign_id"], rows["4"]["account_id"]) == ("331", "1")
        assert (rows["3"]["created_at"], rows["4"]["updated_at"]) == (CREATED, UPDATED)

    def test_a_profile_takes_its_own_entity_row_over_the_facts(self, warehouse):
        first, second = _profile("1", warehouse), _profile("2", warehouse)
        _seed(warehouse, "1", entity=True)
        _seed(warehouse, "2", entity=False)
        _materialize(warehouse, first, second)
        rows = {row["account_id"]: row for row in _rows(warehouse, "dim_accounts")}
        assert set(rows) == {"1", "2"}
        own = rows["1"]
        assert (own["account_name"], own["currency"], own["timezone"]) == ("Acme DE", "EUR", "Europe/Berlin")
        assert (own["marketplace_id"], own["country_code"], own["account_type"]) == ("A1PA6795UKMFR9", "DE", "vendor")
        assert own["as_of_date"] == dt.date(2026, 9, 30)
        other = rows["2"]
        assert (other["account_name"], other["currency"], other["timezone"]) == (None, "EUR", None)
        assert other["as_of_date"] == DAY

    def test_a_missing_profile_table_fails_the_view(self, warehouse):
        first = _profile("1", warehouse)
        star = AmazonAdsStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "campaigns_stats__1" in error

    def test_a_fact_with_no_profile_bound_fails_clearly(self, warehouse):
        star = AmazonAdsStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
