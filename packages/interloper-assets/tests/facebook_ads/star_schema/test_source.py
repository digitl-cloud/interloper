"""Tests for the Facebook Ads star schema: views over every account's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_assets.facebook_ads.connector.source import FacebookAds, FacebookAdsConnection
from interloper_assets.facebook_ads.star_schema import schemas
from interloper_assets.facebook_ads.star_schema import source as star_schema
from interloper_assets.facebook_ads.star_schema.source import FacebookAdsStarSchema

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


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _account(account_id: str, warehouse) -> il.Source:
    connection = FacebookAdsConnection(id="fb", access_token="x", app_id="1", app_secret="x")
    return FacebookAds(account_id=account_id, connection=connection, destinations=[warehouse], enabled=False)


def _seed(
    warehouse,
    account_id: str,
    *,
    campaigns_entity: bool,
    entity_date: dt.date = DAY,
    ads_entity: bool = False,
    extra_fact_campaign: bool = False,
    two_report_rows: bool = False,
    two_entities: bool = False,
    accounts_entity: bool = False,
    adsets_entity: bool = False,
) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS facebook_ads")
    client.execute(
        f"CREATE TABLE facebook_ads.ad_accounts__{account_id} AS SELECT "
        f"DATE '2026-09-30' AS date, '{account_id}' AS account_id, 'Acct from entity' AS name, 'EUR' AS currency, "
        f"'Europe/Berlin' AS timezone_name WHERE {'TRUE' if accounts_entity else 'FALSE'}"
    )
    client.execute(
        f"CREATE TABLE facebook_ads.adsets__{account_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 's1' AS id, 'Set from entity' AS name, 'c{account_id}' AS campaign_id, "
        f"'{account_id}' AS account_id, 'ACTIVE' AS status, CAST(NULL AS VARCHAR) AS daily_budget, "
        "'9000' AS lifetime_budget, '2026-10-01T00:00:00+0200' AS start_time, '' AS end_time, "
        "'2026-09-01T10:00:00+0200' AS created_time, '2026-09-02T10:00:00+0200' AS updated_time "
        f"WHERE {'TRUE' if adsets_entity else 'FALSE'}"
    )
    client.execute(
        f"CREATE TABLE facebook_ads.ads_stats__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date_start, '{account_id}' AS account_id, 'Acct {account_id}' AS account_name, "
        f"'EUR' AS account_currency, 'c{account_id}' AS campaign_id, 'Camp from stats' AS campaign_name, "
        "'s1' AS adset_id, 'Set' AS adset_name, 'a1' AS ad_id, 'Ad' AS ad_name, "
        "'facebook' AS publisher_platform, 'feed' AS platform_position, 'mobile' AS impression_device, "
        "100 AS impressions, 10 AS clicks, 80 AS reach, 12.5 AS spend, 2 AS actions_purchase, "
        "40.0 AS action_values_purchase"
    )
    client.execute(
        f"CREATE TABLE facebook_ads.campaigns_stats__{account_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', {account_id}, 'Acct {account_id}', 'EUR', 'c{account_id}', 'Camp from report', "
        "'OUTCOME_SALES', 100, 10, 80, 12.5, 2, 40.0)"
        + (
            f", (DATE '2026-10-01', {account_id}, 'Acct {account_id}', 'EUR', 'c{account_id}', "
            "'Camp from report renamed', CAST(NULL AS VARCHAR), 50, 5, 20, 2.5, 1, 10.0)"
            if two_report_rows
            else ""
        )
        + ") AS t(date_start, account_id, account_name, account_currency, campaign_id, campaign_name, objective, "
        "impressions, clicks, reach, spend, actions_purchase, action_values_purchase)"
    )
    collected = "TRUE" if campaigns_entity else "FALSE"
    client.execute(
        f"CREATE TABLE facebook_ads.campaigns__{account_id} AS SELECT * FROM (VALUES "
        f"(DATE '{entity_date}', 'c{account_id}', 'Camp from entity', '{account_id}', 'OUTCOME_SALES', 'ACTIVE', "
        "'ACTIVE', 'AUCTION', '1000', CAST(NULL AS VARCHAR), '2026-10-01T00:00:00+0200', '', "
        "'2026-09-01T10:00:00+0200', '2026-09-02T10:00:00+0200')"
        + (
            f", (DATE '{entity_date}', 'c{account_id}b', 'Second from entity', '{account_id}', 'OUTCOME_SALES', "
            "'PAUSED', 'PAUSED', 'AUCTION', CAST(NULL AS VARCHAR), '5000', '2026-10-02T00:00:00+0200', "
            "CAST(NULL AS VARCHAR), '2026-09-01T10:00:00+0200', '2026-09-02T10:00:00+0200')"
            if two_entities
            else ""
        )
        + ") AS t(date, id, name, account_id, objective, status, effective_status, buying_type, daily_budget, "
        f"lifetime_budget, start_time, stop_time, created_time, updated_time) WHERE {collected}"
    )
    ad_collected = "TRUE" if ads_entity else "FALSE"
    client.execute(
        f"CREATE TABLE facebook_ads.ads__{account_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 'a1' AS id, 'Ad from entity' AS name, 's1' AS adset_id, "
        f"'c{account_id}' AS campaign_id, '{account_id}' AS account_id, 'ACTIVE' AS status, "
        "'ACTIVE' AS effective_status, '2026-09-01T10:00:00+0200' AS created_time, "
        f"'2026-09-02T10:00:00+0200' AS updated_time WHERE {ad_collected}"
    )
    if extra_fact_campaign:
        client.execute(
            f"INSERT INTO facebook_ads.campaigns_stats__{account_id} SELECT "
            f"DATE '2026-10-01', {account_id}, 'Acct {account_id}', 'EUR', 'c999', 'Camp from facts', "
            "'OUTCOME_SALES', 1, 1, 1, 1.0, 0, 0.0"
        )


def _materialize(warehouse, *accounts: il.Source) -> il.Source:
    star = FacebookAdsStarSchema(destinations=[warehouse])
    il.DAG(*accounts, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "facebook_ads", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    def test_relations_fan_in_every_account_and_are_optional(self):
        fact = FacebookAdsStarSchema.fact_campaign_performance.relations["campaigns_stats"]
        assert (fact.key, fact.many, fact.optional) == ("facebook_ads.campaigns_stats", True, True)
        assert FacebookAdsStarSchema.dim_campaigns.relations["campaigns"].key == [
            "facebook_ads.campaigns",
            "facebook_ads.campaigns_stats",
        ]
        assert FacebookAdsStarSchema.dim_ad_groups.relations["ads_stats"].key == "facebook_ads.ads_stats"
        assert FacebookAdsStarSchema.dim_ad_groups.relations["adsets"].key == "facebook_ads.adsets"
        assert FacebookAdsStarSchema.dim_accounts.relations["ad_accounts"].key == "facebook_ads.ad_accounts"
        assert FacebookAdsStarSchema.dim_ads.relations["ads"].key == "facebook_ads.ads"

    def test_views_carry_the_core_columns_first(self):
        assert list(schemas.FactCampaignPerformance.model_fields)[:11] == FACT_CORE
        assert list(schemas.DimAccounts.model_fields)[:5] == [
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
        assert "spend" not in schemas.FactAdPerformance.model_fields

    def test_views_are_unpartitioned_and_tagged(self):
        for asset in FacebookAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert FacebookAdsStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert FacebookAdsStarSchema.dim_ad_groups.tags == ["Dimension"]

    def test_the_source_is_shown_under_facebook_ads_in_its_dataset(self):
        assert FacebookAdsStarSchema.group == "facebook_ads"
        star = FacebookAdsStarSchema()
        assert star.dataset == "facebook_ads"
        assert star.fact_campaign_performance.dataset == "facebook_ads"


class TestStamping:
    def test_an_upstream_of_a_source_without_a_discriminator_is_refused(self):
        upstream = il.Upstream(asset=FacebookAdsStarSchema().fact_campaign_performance, data=None)
        with pytest.raises(ConfigError, match="belongs to no account"):
            star_schema._account(upstream)


class TestOnDuckDB:
    def test_the_campaign_fact_sums_to_one_row_per_campaign_and_day(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_entity=True, two_report_rows=True)
        _seed(warehouse, "222", campaigns_entity=False)
        _materialize(warehouse, first, second)
        rows = _rows(warehouse, "fact_campaign_performance")
        assert len(rows) == len({(row["date"], row["account_id"], row["campaign_id"]) for row in rows}) == 2
        [row] = [row for row in rows if row["account_id"] == "111"]
        assert (row["date"], row["campaign_id"]) == (DAY, "c111")
        assert (row["impressions"], row["clicks"], row["cost"], row["currency"]) == (150, 15, 15.0, "EUR")
        assert row["conversions"] is None and row["conversion_value"] is None
        assert (row["purchases"], row["revenue"]) == (3, 50.0)
        assert "reach" not in row

    def test_the_ad_fact_keeps_the_placement_grain_without_names(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False)
        _materialize(warehouse, first)
        frame = warehouse.select("fact_ad_performance", "facebook_ads", None)
        assert list(frame.columns) == list(schemas.FactAdPerformance.model_fields)
        [row] = _rows(warehouse, "fact_ad_performance")
        assert (row["account_id"], row["ad_group_id"], row["ad_id"]) == ("111", "s1", "a1")
        assert (row["cost"], row["purchases"], row["revenue"], row["publisher_platform"]) == (12.5, 2, 40.0, "facebook")

    def test_a_campaign_takes_its_entity_over_its_report_over_the_facts(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_entity=True)
        _seed(warehouse, "222", campaigns_entity=False)
        _materialize(warehouse, first, second)
        rows = {row["account_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        entity = rows["111"]
        assert (entity["campaign_name"], entity["status"], entity["effective_status"]) == (
            "Camp from entity",
            "ACTIVE",
            "ACTIVE",
        )
        assert (entity["objective"], entity["campaign_type"]) == ("OUTCOME_SALES", "AUCTION")
        assert (entity["budget"], entity["budget_type"]) == (None, "daily")
        report = rows["222"]
        assert (report["campaign_name"], report["objective"]) == ("Camp from report", "OUTCOME_SALES")
        assert report["status"] is None and report["budget_type"] is None and report["start_date"] is None

    def test_schedule_dates_come_from_the_local_timestamp_and_blank_is_null(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=True, two_entities=True)
        _materialize(warehouse, first)
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["c111"]["start_date"] == dt.date(2026, 10, 1)
        assert rows["c111"]["end_date"] is None
        assert rows["c111b"]["start_date"] == dt.date(2026, 10, 2)
        assert rows["c111b"]["end_date"] is None
        assert rows["c111b"]["budget_type"] == "lifetime"

    def test_entity_instants_parse_with_the_vendors_offset(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=True)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert row["created_at"] == dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.timezone.utc)
        assert row["updated_at"] == dt.datetime(2026, 9, 2, 8, 0, tzinfo=dt.timezone.utc)

    def test_a_newer_fact_does_not_displace_an_older_entity_row(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=True, entity_date=dt.date(2026, 9, 30))
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert row["campaign_name"] == "Camp from entity"
        assert row["as_of_date"] == dt.date(2026, 9, 30)

    def test_a_campaign_without_an_entity_row_falls_through_to_the_report(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=True, extra_fact_campaign=True)
        _materialize(warehouse, first)
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["c999"]["account_id"] == "111"
        assert rows["c999"]["campaign_name"] == "Camp from facts"
        assert rows["c999"]["status"] is None and rows["c999"]["objective"] == "OUTCOME_SALES"
        assert rows["c111"]["campaign_name"] == "Camp from entity"

    def test_a_campaign_known_only_from_the_facts_comes_from_them(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_entity=True)
        _seed(warehouse, "222", campaigns_entity=False)
        star = FacebookAdsStarSchema(destinations=[warehouse])
        star.dim_campaigns.bind("campaigns", first.campaigns)
        il.DAG(first, second, star).materialize()
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["c111"]["campaign_name"] == "Camp from entity"
        fact_only = rows["c222"]
        assert (fact_only["account_id"], fact_only["as_of_date"]) == ("222", DAY)
        assert fact_only["campaign_name"] is None and fact_only["objective"] is None

    def test_tied_report_rows_collapse_deterministically(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False, two_report_rows=True)
        _materialize(warehouse, first)
        before = _rows(warehouse, "dim_campaigns")
        [row] = before
        assert (row["campaign_name"], row["objective"]) == ("Camp from report renamed", "OUTCOME_SALES")
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_campaigns") == before

    def test_ad_groups_come_from_the_report_then_the_facts(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ad_groups")
        assert (row["ad_group_id"], row["campaign_id"], row["account_id"]) == ("s1", "c111", "111")
        assert (row["ad_group_name"], row["status"], row["budget"], row["start_date"]) == ("Set", None, None, None)
        assert row["as_of_date"] == DAY

    def test_an_ad_entity_wins_over_the_report(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False, ads_entity=True)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ads")
        assert (row["ad_name"], row["status"], row["effective_status"]) == ("Ad from entity", "ACTIVE", "ACTIVE")
        assert (row["ad_group_id"], row["campaign_id"], row["account_id"]) == ("s1", "c111", "111")
        assert row["as_of_date"] == dt.date(2026, 9, 30)

    def test_ads_come_from_the_report_without_an_entity(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False)
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_ads") == [
            {
                "ad_id": "a1",
                "ad_group_id": "s1",
                "campaign_id": "c111",
                "account_id": "111",
                "ad_name": "Ad",
                "status": None,
                "created_at": None,
                "updated_at": None,
                "as_of_date": DAY,
                "effective_status": None,
            }
        ]

    def test_accounts_take_the_ad_accounts_entity_with_its_timezone(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False, accounts_entity=True)
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {
                "account_id": "111",
                "account_name": "Acct from entity",
                "currency": "EUR",
                "timezone": "Europe/Berlin",
                "as_of_date": dt.date(2026, 9, 30),
            }
        ]

    def test_ad_groups_take_the_adsets_entity(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False, adsets_entity=True)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ad_groups")
        assert (row["ad_group_id"], row["campaign_id"], row["account_id"]) == ("s1", "c111", "111")
        assert (row["ad_group_name"], row["status"], row["budget"], row["budget_type"]) == (
            "Set from entity",
            "ACTIVE",
            None,
            "lifetime",
        )
        assert (row["start_date"], row["end_date"]) == (dt.date(2026, 10, 1), None)
        assert row["created_at"] == dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.timezone.utc)
        assert row["as_of_date"] == dt.date(2026, 9, 30)

    def test_accounts_come_from_the_report_then_the_fact(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False)
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "111", "account_name": "Acct 111", "currency": "EUR", "timezone": None, "as_of_date": DAY}
        ]

    def test_the_views_are_read_through_the_read_path(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_entity=False)
        star = _materialize(warehouse, first)
        frame = warehouse.read(il.IOContext(asset=star.dim_campaigns))
        assert frame["campaign_id"].tolist() == ["c111"]

    def test_a_missing_account_table_fails_the_view(self, warehouse):
        first = _account("111", warehouse)
        star = FacebookAdsStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "campaigns_stats__111" in error

    def test_a_fact_with_no_account_bound_fails_clearly(self, warehouse):
        star = FacebookAdsStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
