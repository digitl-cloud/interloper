"""Tests for the Bing Ads star schema: views over every account's ad report, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.connector.source import BingAds
from interloper_assets.bing_ads.star_schema import schemas
from interloper_assets.bing_ads.star_schema import source as star_schema
from interloper_assets.bing_ads.star_schema.source import BingAdsStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)
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
REPORT_COLUMNS = (
    "time_period, account_id, account_name, customer_id, currency_code, campaign_id, campaign_name, "
    "campaign_status, campaign_type, ad_group_id, ad_group_name, ad_group_status, ad_id, ad_status, ad_type, "
    "headline, device_type, goal, impressions, clicks, spend, conversions, revenue"
)


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _account(account_id: str, warehouse) -> il.Source:
    connection = BingAdsConnection(client_id="cid", client_secret="x", refresh_token="x", developer_token="x")
    return BingAds(account_id=account_id, connection=connection, destinations=[warehouse], enabled=False)


def _row(
    account_id: str,
    ad_id: str = "a1",
    *,
    day: str = "2026-10-01",
    device: str = "Computer",
    goal: str = "NULL",
    impressions: int = 100,
    clicks: int = 10,
    spend: float = 12.5,
    conversions: int = 0,
    revenue: float = 0.0,
    campaign_name: str = "Camp",
    status: str = "Active",
) -> str:
    return (
        f"(DATE '{day}', '{account_id}', 'Acct {account_id}', 'cust1', 'EUR', 'c{account_id}', '{campaign_name}', "
        f"'{status}', 'Search', 's{account_id}', 'Group', 'Active', '{ad_id}', 'Active', 'Responsive search ad', "
        f"'Shop now', '{device}', {goal}, {impressions}, {clicks}, {spend}, {conversions}, {revenue})"
    )


def _seed(warehouse, account_id: str, rows: list[str] | None = None) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS bing_ads")
    rows = rows or [_row(account_id)]
    client.execute(
        f"CREATE TABLE bing_ads.ads_stats__{account_id} AS SELECT * FROM (VALUES {', '.join(rows)}) "
        f"AS t({REPORT_COLUMNS})"
    )


def _materialize(warehouse, *accounts: il.Source) -> il.Source:
    star = BingAdsStarSchema(destinations=[warehouse])
    il.DAG(*accounts, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "bing_ads", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    @pytest.mark.parametrize(
        "view", ["fact_campaign_performance", "fact_ad_performance", "dim_campaigns", "dim_ad_groups", "dim_ads"]
    )
    def test_the_ad_report_fans_in_every_account_and_is_optional(self, view: str):
        relation = getattr(BingAdsStarSchema, view).relations["ads_stats"]
        assert (relation.key, relation.many, relation.optional) == ("bing_ads.ads_stats", True, True)

    def test_views_carry_the_core_columns_first(self):
        assert list(schemas.FactCampaignPerformance.model_fields) == FACT_CORE
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
        assert list(schemas.DimAds.model_fields)[:9] == [
            "ad_id",
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_name",
            "status",
            "created_at",
            "updated_at",
            "as_of_date",
        ]

    def test_views_are_unpartitioned_and_tagged(self):
        for asset in BingAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert BingAdsStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert BingAdsStarSchema.dim_ads.tags == ["Dimension"]


class TestStamping:
    def test_an_upstream_of_a_source_without_a_discriminator_is_refused(self):
        upstream = il.Upstream(asset=BingAdsStarSchema().fact_campaign_performance, data=None)
        with pytest.raises(ConfigError, match="belongs to no account"):
            star_schema._account(upstream)


class TestOnDuckDB:
    def test_the_campaign_fact_sums_every_breakdown_and_goal_row(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        rows = [
            _row("111", device="Computer"),
            _row("111", device="Smartphone", impressions=50, clicks=5, spend=2.5),
            # Bing reports a goal's conversions on a row of their own, with no delivery
            _row("111", goal="'Online purchase'", impressions=0, clicks=0, spend=0.0, conversions=3, revenue=90.0),
        ]
        _seed(warehouse, "111", rows)
        _seed(warehouse, "222")
        _materialize(warehouse, first, second)
        facts = _rows(warehouse, "fact_campaign_performance")
        assert len(facts) == len({(row["date"], row["account_id"], row["campaign_id"]) for row in facts}) == 2
        [row] = [row for row in facts if row["account_id"] == "111"]
        assert (row["date"], row["campaign_id"], row["impressions"], row["clicks"]) == (DAY, "c111", 150, 15)
        assert (row["cost"], row["currency"], row["conversions"], row["conversion_value"]) == (15.0, "EUR", 3, 90.0)
        assert row["purchases"] is None and row["revenue"] is None

    def test_the_ad_fact_keeps_one_row_per_ad_and_day(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", [_row("111"), _row("111", device="Tablet", impressions=1)])
        _materialize(warehouse, first)
        frame = warehouse.select("fact_ad_performance", "bing_ads", None)
        assert list(frame.columns) == list(schemas.FactAdPerformance.model_fields)
        [row] = _rows(warehouse, "fact_ad_performance")
        assert (row["campaign_id"], row["ad_group_id"], row["ad_id"], row["impressions"]) == ("c111", "s111", "a1", 101)

    def test_an_account_takes_its_name_and_customer_from_the_latest_report(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", [_row("111"), _row("111", day="2026-09-30")])
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {
                "account_id": "111",
                "account_name": "Acct 111",
                "currency": "EUR",
                "timezone": None,
                "as_of_date": DAY,
                "customer_id": "cust1",
            }
        ]

    def test_a_campaign_takes_its_name_status_and_type_from_its_latest_report(self, warehouse):
        first = _account("111", warehouse)
        rows = [_row("111", day="2026-09-30", campaign_name="Old", status="Paused"), _row("111", campaign_name="New")]
        _seed(warehouse, "111", rows)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert (row["campaign_name"], row["status"], row["campaign_type"], row["as_of_date"]) == (
            "New",
            "Active",
            "Search",
            DAY,
        )
        assert row["budget"] is None and row["start_date"] is None

    def test_ad_groups_and_ads_take_their_names_and_parents_from_the_report(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111")
        _materialize(warehouse, first)
        [group] = _rows(warehouse, "dim_ad_groups")
        assert (group["ad_group_name"], group["campaign_id"], group["status"]) == ("Group", "c111", "Active")
        [ad] = _rows(warehouse, "dim_ads")
        assert (ad["ad_group_id"], ad["campaign_id"], ad["ad_name"], ad["ad_type"], ad["headline"]) == (
            "s111",
            "c111",
            None,
            "Responsive search ad",
            "Shop now",
        )

    def test_tied_report_rows_collapse_deterministically(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", [_row("111", campaign_name="Name A"), _row("111", campaign_name="Name B")])
        _materialize(warehouse, first)
        assert {_rows(warehouse, "dim_campaigns")[0]["campaign_name"] for _ in range(5)} == {"Name B"}

    def test_every_key_a_fact_carries_resolves_in_its_dimension(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", [_row("111"), _row("111", ad_id="a2")])
        _seed(warehouse, "222")
        _materialize(warehouse, first, second)
        ads = _rows(warehouse, "fact_ad_performance")
        assert {row["ad_id"] for row in ads} <= {row["ad_id"] for row in _rows(warehouse, "dim_ads")}
        assert {row["ad_group_id"] for row in ads} <= {row["ad_group_id"] for row in _rows(warehouse, "dim_ad_groups")}
        assert {row["account_id"] for row in _rows(warehouse, "dim_accounts")} == {"111", "222"}

    def test_a_missing_account_table_fails_the_view(self, warehouse):
        first = _account("111", warehouse)
        star = BingAdsStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "ads_stats__111" in error

    def test_a_fact_with_no_account_bound_fails_clearly(self, warehouse):
        star = BingAdsStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
