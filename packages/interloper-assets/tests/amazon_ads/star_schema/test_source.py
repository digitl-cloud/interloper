"""Tests for the Amazon Ads star schema: views over every profile's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.runner import ExecutionStatus

from interloper_assets.amazon_ads.connection import AmazonAdsConnection
from interloper_assets.amazon_ads.connector.source import AmazonAds
from interloper_assets.amazon_ads.star_schema.source import AmazonAdsStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _profile(profile_id: str, warehouse) -> il.Source:
    connection = AmazonAdsConnection(id="amz", client_id="x", client_secret="x", refresh_token="x", location="EU")
    return AmazonAds(profile_id=profile_id, connection=connection, destinations=[warehouse], enabled=False)


def _seed(warehouse, profile_id: str, *, entity: bool) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS amazon_ads")
    client.execute(
        f"CREATE TABLE amazon_ads.products_campaigns_stats__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 11{profile_id}, 'SP campaign', 'ENABLED', 50.0, 'EUR', 100, 10, 12.5, 2, 40.0, 1), "
        f"(DATE '2026-10-01', 11{profile_id}, 'SP campaign', 'ENABLED', 50.0, 'EUR', 50, 5, 2.5, 1, 10.0, 2)"
        ") AS t(date, campaign_id, campaign_name, campaign_status, campaign_budget_amount, "
        "campaign_budget_currency_code, impressions, clicks, spend, purchases_14d, sales_14d, ad_group_id)"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.brands_campaigns_stats__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 22{profile_id}, 'SB campaign', 'PAUSED', 20.0, 'EUR', 30, 3, 4.0, 1, 15.0)"
        ") AS t(date, campaign_id, campaign_name, campaign_status, campaign_budget_amount, "
        "campaign_budget_currency_code, impressions, clicks, cost, purchases, sales)"
    )
    client.execute(
        f"CREATE TABLE amazon_ads.display_campaigns_stats__{profile_id} (date DATE, campaign_id BIGINT, "
        "campaign_name VARCHAR, campaign_status VARCHAR, campaign_budget_amount DOUBLE, "
        "campaign_budget_currency_code VARCHAR, impressions BIGINT, clicks BIGINT, cost DOUBLE, "
        "purchases BIGINT, sales DOUBLE)"
    )
    collected = "TRUE" if entity else "FALSE"
    client.execute(
        f"CREATE TABLE amazon_ads.profiles__{profile_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-09-30', {profile_id}, 'DE', 'EUR', 'Europe/Berlin', 'A1PA6795UKMFR9', 'acc', 'vendor', "
        "'Acme DE', TRUE)"
        ") AS t(date, profile_id, country_code, currency_code, timezone, account_info_marketplace_string_id, "
        f"account_info_id, account_info_type, account_info_name, account_info_valid_payment_method) WHERE {collected}"
    )


def _materialize(warehouse, *profiles: il.Source) -> il.Source:
    star = AmazonAdsStarSchema(destinations=[warehouse])
    il.DAG(*profiles, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "amazon_ads", None)
    if "date" in frame:
        frame["date"] = frame["date"].dt.date
    return frame.sort_values(list(frame.columns)).to_dict("records")


class TestWiring:
    def test_raw_upstreams_fan_in_every_profile_and_are_optional(self):
        fact = AmazonAdsStarSchema.fact_campaign_performance.relations["campaigns_stats"]
        assert fact.key == [
            "amazon_ads.products_campaigns_stats",
            "amazon_ads.brands_campaigns_stats",
            "amazon_ads.display_campaigns_stats",
        ]
        assert (fact.many, fact.optional) == (True, True)
        profiles = AmazonAdsStarSchema.dim_accounts.relations["profiles"]
        assert (profiles.key, profiles.many, profiles.optional) == ("amazon_ads.profiles", True, True)

    def test_views_are_unpartitioned_tagged_and_grouped_under_amazon_ads(self):
        for asset in AmazonAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert AmazonAdsStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert AmazonAdsStarSchema.group == "amazon_ads"
        assert AmazonAdsStarSchema().dataset == "amazon_ads"


class TestOnDuckDB:
    def test_the_fact_unions_every_profile_and_ad_product_at_campaign_grain(self, warehouse):
        first, second = _profile("1", warehouse), _profile("2", warehouse)
        _seed(warehouse, "1", entity=True)
        _seed(warehouse, "2", entity=False)
        _materialize(warehouse, first, second)
        rows = {(row["profile_id"], row["ad_product"]): row for row in _rows(warehouse, "fact_campaign_performance")}
        assert set(rows) == {
            ("1", "sponsored_products"),
            ("1", "sponsored_brands"),
            ("2", "sponsored_products"),
            ("2", "sponsored_brands"),
        }
        products = rows[("1", "sponsored_products")]
        assert products["campaign_id"] == "111"
        assert (products["impressions"], products["clicks"]) == (150, 15)
        assert (products["cost"], products["purchases"], products["sales"]) == (15.0, 3, 50.0)
        brands = rows[("1", "sponsored_brands")]
        assert (brands["campaign_id"], brands["cost"], brands["purchases"], brands["sales"]) == ("221", 4.0, 1, 15.0)
        assert products["date"] == DAY

    def test_campaigns_come_from_the_facts(self, warehouse):
        first = _profile("1", warehouse)
        _seed(warehouse, "1", entity=False)
        _materialize(warehouse, first)
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["111"]["campaign_name"] == "SP campaign"
        assert rows["111"]["ad_product"] == "sponsored_products"
        assert rows["221"]["campaign_status"] == "PAUSED"
        assert rows["221"]["profile_id"] == "1"

    def test_a_profile_takes_its_entity_row_over_the_facts(self, warehouse):
        first, second = _profile("1", warehouse), _profile("2", warehouse)
        _seed(warehouse, "1", entity=True)
        _seed(warehouse, "2", entity=False)
        _materialize(warehouse, first, second)
        rows = {row["profile_id"]: row for row in _rows(warehouse, "dim_accounts")}
        assert rows["1"]["account_name"] == "Acme DE"
        assert rows["1"]["marketplace_id"] == "A1PA6795UKMFR9"
        assert rows["1"]["date"] == dt.date(2026, 9, 30)
        assert rows["2"]["account_name"] is None
        assert rows["2"]["currency_code"] == "EUR"
        assert rows["2"]["date"] == DAY

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
