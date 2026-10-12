"""Tests for the Facebook Ads star schema: views over every account's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.runner.results import ExecutionStatus

from interloper_assets.facebook_ads.source import FacebookAds, FacebookAdsConnection
from interloper_assets.facebook_ads_star_schema.source import FacebookAdsStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _account(account_id: str, warehouse) -> il.Source:
    connection = FacebookAdsConnection(id="fb", access_token="x", app_id="1", app_secret="x")
    return FacebookAds(account_id=account_id, connection=connection, destinations=[warehouse], select=[])


def _seed(warehouse, account_id: str, *, campaigns_snapshot: bool) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS facebook_ads")
    client.execute(
        f"CREATE TABLE facebook_ads.ads_stats__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date_start, '{account_id}' AS account_id, 'Acct {account_id}' AS account_name, "
        f"'EUR' AS account_currency, 'c{account_id}' AS campaign_id, 'Camp from stats' AS campaign_name, "
        "'s1' AS adset_id, 'Set' AS adset_name, 'a1' AS ad_id, 'Ad' AS ad_name, "
        "'facebook' AS publisher_platform, 'feed' AS platform_position, 'mobile' AS impression_device, "
        "100 AS impressions, 10 AS clicks, 80 AS reach, 12.5 AS spend"
    )
    client.execute(
        f"CREATE TABLE facebook_ads.campaigns_stats__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date_start, {account_id} AS account_id, 'c{account_id}' AS campaign_id, "
        "'Camp from report' AS campaign_name, 'OUTCOME_SALES' AS objective"
    )
    collected = "TRUE" if campaigns_snapshot else "FALSE"
    client.execute(
        f"CREATE TABLE facebook_ads.campaigns__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date, 'c{account_id}' AS id, 'Camp from snapshot' AS name, "
        f"'{account_id}' AS account_id, 'OUTCOME_SALES' AS objective, 'ACTIVE' AS status WHERE {collected}"
    )
    client.execute(
        f"CREATE TABLE facebook_ads.ads__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date, 'a1' AS id, 'Ad' AS name, 's1' AS adset_id, 'c{account_id}' AS campaign_id, "
        f"'{account_id}' AS account_id, 'ACTIVE' AS status WHERE FALSE"
    )


def _materialize(warehouse, *accounts: il.Source) -> il.Source:
    star = FacebookAdsStarSchema(destinations=[warehouse])
    il.DAG(*accounts, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "facebook_ads_star_schema", None)
    if "date" in frame:
        frame["date"] = frame["date"].dt.date
    return frame.sort_values(list(frame.columns)).to_dict("records")


class TestWiring:
    def test_relations_are_the_placeholders(self):
        fact = FacebookAdsStarSchema.fact_ad_performance.relations["ads_stats"]  # ty: ignore[unresolved-attribute]
        assert (fact.key, fact.many, fact.optional) == ("facebook_ads.ads_stats", True, True)
        campaigns = FacebookAdsStarSchema.dim_campaigns.relations["campaigns"]  # ty: ignore[unresolved-attribute]
        assert campaigns.key == ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"]
        assert FacebookAdsStarSchema.dim_accounts.relations["fact_ad_performance"].key == "fact_ad_performance"  # ty: ignore[unresolved-attribute]

    def test_views_are_unpartitioned_and_tagged(self):
        for asset in FacebookAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert FacebookAdsStarSchema.fact_ad_performance.tags == ["Fact"]  # ty: ignore[unresolved-attribute]
        assert FacebookAdsStarSchema.dim_campaigns.tags == ["Dimension"]  # ty: ignore[unresolved-attribute]


class TestOnDuckDB:
    def test_the_fact_unions_every_account(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=True)
        _seed(warehouse, "222", campaigns_snapshot=False)
        _materialize(warehouse, first, second)
        rows = _rows(warehouse, "fact_ad_performance")
        assert [row["account_id"] for row in rows] == ["111", "222"]
        assert rows[0]["spend"] == 12.5 and rows[0]["date"] == DAY

    def test_a_campaign_takes_its_snapshot_over_its_report_over_the_facts(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=True)
        _seed(warehouse, "222", campaigns_snapshot=False)
        _materialize(warehouse, first, second)
        rows = {row["account_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["111"]["campaign_name"] == "Camp from snapshot"
        assert rows["111"]["status"] == "ACTIVE"
        assert rows["222"]["campaign_name"] == "Camp from report"
        assert rows["222"]["status"] is None

    def test_ads_and_accounts_come_from_the_facts(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=False)
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_ads") == [
            {
                "ad_id": "a1",
                "ad_name": "Ad",
                "adset_id": "s1",
                "adset_name": "Set",
                "campaign_id": "c111",
                "account_id": "111",
                "status": None,
                "date": DAY,
            }
        ]
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "111", "account_name": "Acct 111", "account_currency": "EUR", "date": DAY}
        ]

    def test_the_views_are_read_through_the_read_path(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=False)
        star = _materialize(warehouse, first)
        frame = warehouse.read(il.IOContext(asset=star.dim_campaigns))
        assert frame["campaign_id"].tolist() == ["c111"]

    def test_a_missing_account_table_fails_the_view(self, warehouse):
        first = _account("111", warehouse)
        star = FacebookAdsStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_ad_performance.id].error
        assert error is not None and "ads_stats__111" in error

    def test_a_fact_with_no_account_bound_fails_clearly(self, warehouse):
        star = FacebookAdsStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_ad_performance.id].error
        assert error is not None and "no upstream bound" in error
