"""Tests for the TikTok Ads star schema: views over every advertiser's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_assets.tiktok_ads.connection import TiktokAdsConnection
from interloper_assets.tiktok_ads.connector.source import TiktokAds
from interloper_assets.tiktok_ads.star_schema import schemas
from interloper_assets.tiktok_ads.star_schema import source as star_schema
from interloper_assets.tiktok_ads.star_schema.source import TiktokAdsStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)
SNAPSHOT = dt.date(2026, 9, 30)
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
    "stat_time_day, campaign_id, campaign_name, adgroup_id, adgroup_name, ad_id, ad_name, currency, "
    "impressions, clicks, reach, spend, complete_payment, total_complete_payment_rate"
)


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _advertiser(advertiser_id: str, warehouse) -> il.Source:
    connection = TiktokAdsConnection(id="tt", access_token="x")
    return TiktokAds(advertiser_id=advertiser_id, connection=connection, destinations=[warehouse], enabled=False)


def _when(collected: bool) -> str:
    return "TRUE" if collected else "FALSE"


def _report_row(advertiser_id: str, ad_id: str, *, spend: float = 12.5, name: str = "Ad") -> str:
    return (
        f"(DATE '2026-10-01', 'c{advertiser_id}', 'Camp from report', 's{advertiser_id}', 'Group from report', "
        f"'{ad_id}', '{name}', 'EUR', 100, 10, 80, {spend}, 2, 40.0)"
    )


def _seed(
    warehouse,
    advertiser_id: str,
    *,
    entities: bool = True,
    advertiser_entity: bool = True,
    timezone: str = "Europe/Berlin",
    report_rows: list[str] | None = None,
) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS tiktok_ads")
    client.execute(
        f"CREATE TABLE tiktok_ads.advertisers__{advertiser_id} AS SELECT "
        f"DATE '{SNAPSHOT}' AS date, '{advertiser_id}' AS advertiser_id, 'Adv {advertiser_id}' AS name, "
        f"'EUR' AS currency, '{timezone}' AS timezone WHERE {_when(advertiser_entity)}"
    )
    client.execute(
        f"CREATE TABLE tiktok_ads.campaigns__{advertiser_id} AS SELECT "
        f"DATE '{SNAPSHOT}' AS date, 'c{advertiser_id}' AS campaign_id, 'Camp from entity' AS campaign_name, "
        "'ENABLE' AS operation_status, 'CAMPAIGN_STATUS_ENABLE' AS secondary_status, "
        "'WEB_CONVERSIONS' AS objective_type, 'REGULAR_CAMPAIGN' AS campaign_type, 100.0 AS budget, "
        "'BUDGET_MODE_DAY' AS budget_mode, TIMESTAMPTZ '2026-09-01 08:00:00+00' AS create_time, "
        f"TIMESTAMPTZ '2026-09-02 08:00:00+00' AS modify_time WHERE {_when(entities)}"
    )
    client.execute(
        f"CREATE TABLE tiktok_ads.adgroups__{advertiser_id} AS SELECT "
        f"DATE '{SNAPSHOT}' AS date, 's{advertiser_id}' AS adgroup_id, 'c{advertiser_id}' AS campaign_id, "
        "'Group from entity' AS adgroup_name, 'DISABLE' AS operation_status, "
        "'ADGROUP_STATUS_DISABLE' AS secondary_status, 50.0 AS budget, 'BUDGET_MODE_TOTAL' AS budget_mode, "
        "TIMESTAMPTZ '2026-09-30 22:00:00+00' AS schedule_start_time, "
        "TIMESTAMPTZ '2026-10-31 22:59:59+00' AS schedule_end_time, "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00' AS create_time, "
        f"TIMESTAMPTZ '2026-09-02 08:00:00+00' AS modify_time WHERE {_when(entities)}"
    )
    client.execute(
        f"CREATE TABLE tiktok_ads.ads__{advertiser_id} AS SELECT "
        f"DATE '{SNAPSHOT}' AS date, 'a{advertiser_id}' AS ad_id, 's{advertiser_id}' AS adgroup_id, "
        f"'c{advertiser_id}' AS campaign_id, 'Ad from entity' AS ad_name, 'ENABLE' AS operation_status, "
        "'AD_STATUS_DELIVERY_OK' AS secondary_status, TIMESTAMPTZ '2026-09-01 08:00:00+00' AS create_time, "
        f"TIMESTAMPTZ '2026-09-02 08:00:00+00' AS modify_time WHERE {_when(entities)}"
    )
    rows = report_rows or [_report_row(advertiser_id, f"a{advertiser_id}")]
    client.execute(
        f"CREATE TABLE tiktok_ads.ads_stats__{advertiser_id} AS SELECT * FROM (VALUES {', '.join(rows)}) "
        f"AS t({REPORT_COLUMNS})"
    )


def _materialize(warehouse, *advertisers: il.Source) -> il.Source:
    star = TiktokAdsStarSchema(destinations=[warehouse])
    il.DAG(*advertisers, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "tiktok_ads", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    @pytest.mark.parametrize(
        ("view", "name", "key"),
        [
            ("fact_campaign_performance", "ads_stats", "tiktok_ads.ads_stats"),
            ("fact_ad_performance", "ads_stats", "tiktok_ads.ads_stats"),
            ("dim_accounts", "advertisers", "tiktok_ads.advertisers"),
            ("dim_campaigns", "campaigns", "tiktok_ads.campaigns"),
            ("dim_campaigns", "ads_stats", "tiktok_ads.ads_stats"),
            ("dim_ad_groups", "adgroups", "tiktok_ads.adgroups"),
            ("dim_ad_groups", "ads_stats", "tiktok_ads.ads_stats"),
            ("dim_ads", "ads", "tiktok_ads.ads"),
            ("dim_ads", "ads_stats", "tiktok_ads.ads_stats"),
        ],
    )
    def test_raw_upstreams_fan_in_every_advertiser_and_are_optional(self, view: str, name: str, key: str):
        relation = getattr(TiktokAdsStarSchema, view).relations[name]
        assert (relation.key, relation.many, relation.optional) == (key, True, True)

    def test_views_carry_the_core_columns_first(self):
        assert list(schemas.FactCampaignPerformance.model_fields) == FACT_CORE
        assert list(schemas.DimAccounts.model_fields) == [
            "account_id",
            "account_name",
            "currency",
            "timezone",
            "as_of_date",
        ]
        assert list(schemas.DimCampaigns.model_fields)[:13] == [
            "campaign_id",
            "account_id",
            "campaign_name",
            "status",
            "objective",
            "campaign_type",
            *ATTRIBUTE_CORE,
        ]
        assert list(schemas.DimAdGroups.model_fields)[:12] == [
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
        for asset in TiktokAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert TiktokAdsStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert TiktokAdsStarSchema.dim_ads.tags == ["Dimension"]


class TestStamping:
    def test_an_upstream_of_a_source_without_a_discriminator_is_refused(self):
        upstream = il.Upstream(asset=TiktokAdsStarSchema().fact_campaign_performance, data=None)
        with pytest.raises(ConfigError, match="belongs to no account"):
            star_schema._account(upstream)


class TestOnDuckDB:
    def test_the_campaign_fact_sums_the_ad_report_to_one_row_per_campaign_and_day(self, warehouse):
        first, second = _advertiser("111", warehouse), _advertiser("222", warehouse)
        rows = [_report_row("111", "a111"), _report_row("111", "a112", spend=2.5)]
        _seed(warehouse, "111", report_rows=rows)
        _seed(warehouse, "222")
        _materialize(warehouse, first, second)
        facts = _rows(warehouse, "fact_campaign_performance")
        assert len(facts) == len({(row["date"], row["account_id"], row["campaign_id"]) for row in facts}) == 2
        [row] = [row for row in facts if row["account_id"] == "111"]
        assert (row["date"], row["campaign_id"], row["impressions"], row["clicks"]) == (DAY, "c111", 200, 20)
        assert (row["cost"], row["currency"], row["purchases"], row["revenue"]) == (15.0, "EUR", 4, 80.0)
        assert row["conversions"] is None and row["conversion_value"] is None
        assert "reach" not in row

    def test_the_ad_fact_keeps_the_ads_keys_and_reach(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed(warehouse, "111")
        _materialize(warehouse, first)
        frame = warehouse.select("fact_ad_performance", "tiktok_ads", None)
        assert list(frame.columns) == list(schemas.FactAdPerformance.model_fields)
        [row] = _rows(warehouse, "fact_ad_performance")
        assert (row["campaign_id"], row["ad_group_id"], row["ad_id"], row["reach"]) == ("c111", "s111", "a111", 80)

    def test_an_advertiser_takes_its_entity_row_else_the_facts(self, warehouse):
        first, second = _advertiser("111", warehouse), _advertiser("222", warehouse)
        _seed(warehouse, "111")
        _seed(warehouse, "222", advertiser_entity=False)
        _materialize(warehouse, first, second)
        rows = {row["account_id"]: row for row in _rows(warehouse, "dim_accounts")}
        assert (rows["111"]["account_name"], rows["111"]["timezone"], rows["111"]["as_of_date"]) == (
            "Adv 111",
            "Europe/Berlin",
            SNAPSHOT,
        )
        assert (rows["222"]["account_name"], rows["222"]["currency"], rows["222"]["as_of_date"]) == (None, "EUR", DAY)

    def test_a_campaign_takes_its_entity_row_over_the_report(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed(warehouse, "111")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert (row["campaign_name"], row["status"], row["effective_status"]) == (
            "Camp from entity",
            "ENABLE",
            "CAMPAIGN_STATUS_ENABLE",
        )
        assert (row["objective"], row["campaign_type"], row["budget"], row["budget_type"]) == (
            "WEB_CONVERSIONS",
            "REGULAR_CAMPAIGN",
            100.0,
            "BUDGET_MODE_DAY",
        )
        assert (row["start_date"], row["end_date"]) == (None, None)
        assert row["updated_at"] == dt.datetime(2026, 9, 2, 8, 0, tzinfo=dt.timezone.utc)

    def test_without_entities_the_dimensions_take_the_reports_names(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed(warehouse, "111", entities=False)
        _materialize(warehouse, first)
        [campaign] = _rows(warehouse, "dim_campaigns")
        assert (campaign["campaign_name"], campaign["status"], campaign["as_of_date"]) == (
            "Camp from report",
            None,
            DAY,
        )
        [group] = _rows(warehouse, "dim_ad_groups")
        assert (group["ad_group_name"], group["campaign_id"], group["start_date"]) == (
            "Group from report",
            "c111",
            None,
        )
        [ad] = _rows(warehouse, "dim_ads")
        assert (ad["ad_name"], ad["ad_group_id"], ad["campaign_id"]) == ("Ad", "s111", "c111")

    def test_an_ad_group_schedule_is_read_in_the_advertisers_days(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed(warehouse, "111")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ad_groups")
        assert (row["ad_group_name"], row["status"], row["budget"], row["budget_type"]) == (
            "Group from entity",
            "DISABLE",
            50.0,
            "BUDGET_MODE_TOTAL",
        )
        # 22:00 UTC on 30 September is 1 October in Berlin
        assert (row["start_date"], row["end_date"]) == (dt.date(2026, 10, 1), dt.date(2026, 10, 31))

    def test_without_the_advertisers_timezone_the_schedule_is_null(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed(warehouse, "111", advertiser_entity=False)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ad_groups")
        assert row["ad_group_name"] == "Group from entity"
        assert (row["start_date"], row["end_date"]) == (None, None)

    def test_an_ad_takes_its_entity_row(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed(warehouse, "111")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ads")
        assert (row["ad_name"], row["ad_group_id"], row["status"], row["effective_status"]) == (
            "Ad from entity",
            "s111",
            "ENABLE",
            "AD_STATUS_DELIVERY_OK",
        )

    def test_tied_report_rows_collapse_deterministically(self, warehouse):
        first = _advertiser("111", warehouse)
        rows = [_report_row("111", "a111", name="Name A"), _report_row("111", "a111", name="Name B")]
        _seed(warehouse, "111", entities=False, report_rows=rows)
        _materialize(warehouse, first)
        assert {_rows(warehouse, "dim_ads")[0]["ad_name"] for _ in range(5)} == {"Name B"}

    def test_every_key_a_fact_carries_resolves_in_its_dimension(self, warehouse):
        first, second = _advertiser("111", warehouse), _advertiser("222", warehouse)
        _seed(warehouse, "111", report_rows=[_report_row("111", "a111"), _report_row("111", "a_report_only")])
        _seed(warehouse, "222", entities=False)
        _materialize(warehouse, first, second)
        ads = _rows(warehouse, "fact_ad_performance")
        assert {row["ad_id"] for row in ads} <= {row["ad_id"] for row in _rows(warehouse, "dim_ads")}
        assert {row["ad_group_id"] for row in ads} <= {row["ad_group_id"] for row in _rows(warehouse, "dim_ad_groups")}
        assert {row["campaign_id"] for row in _rows(warehouse, "fact_campaign_performance")} <= {
            row["campaign_id"] for row in _rows(warehouse, "dim_campaigns")
        }

    def test_a_missing_advertiser_table_fails_the_view(self, warehouse):
        first = _advertiser("111", warehouse)
        star = TiktokAdsStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "ads_stats__111" in error

    def test_a_fact_with_no_advertiser_bound_fails_clearly(self, warehouse):
        star = TiktokAdsStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
