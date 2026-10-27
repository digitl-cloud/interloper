"""Tests for the Snapchat Ads star schema: views over every account's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.connector.source import SnapchatAds
from interloper_assets.snapchat_ads.star_schema import schemas
from interloper_assets.snapchat_ads.star_schema import source as star_schema
from interloper_assets.snapchat_ads.star_schema.source import SnapchatAdsStarSchema

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
    connection = SnapchatAdsConnection(id="snap", client_id="x", client_secret="x", refresh_token="x")
    return SnapchatAds(account_id=account_id, connection=connection, destinations=[warehouse], enabled=False)


def _when(collected: bool) -> str:
    return "TRUE" if collected else "FALSE"


def _seed(
    warehouse,
    account_id: str,
    *,
    accounts_entity: bool = True,
    timezone: str = "Europe/Berlin",
    campaigns_entity: bool = True,
    ad_squads_entity: bool = True,
    ads_entity: bool = True,
    two_campaign_rows: bool = False,
    extra_fact_campaign: bool = False,
) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS snapchat_ads")
    client.execute(
        f"CREATE TABLE snapchat_ads.ad_accounts__{account_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-09-29', '{account_id}', 'Old name', 'USD', '{timezone}'), "
        f"(DATE '2026-09-30', '{account_id}', 'Acct {account_id}', 'EUR', '{timezone}')"
        f") AS t(date, id, name, currency, timezone) WHERE {_when(accounts_entity)}"
    )
    client.execute(
        f"CREATE TABLE snapchat_ads.campaigns__{account_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 'c{account_id}' AS id, 'Camp from entity' AS name, "
        f"'{account_id}' AS ad_account_id, 'ACTIVE' AS status, 'WEB_CONVERSION' AS objective, "
        "'AUCTION' AS buy_model, CAST(17500000000 AS DOUBLE) AS lifetime_spend_cap_micro, "
        "TIMESTAMPTZ '2026-09-30 22:00:00+00' AS start_time, CAST(NULL AS TIMESTAMPTZ) AS end_time, "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00' AS created_at, TIMESTAMPTZ '2026-09-02 08:00:00+00' AS updated_at "
        f"WHERE {_when(campaigns_entity)}"
    )
    client.execute(
        f"CREATE TABLE snapchat_ads.ad_squads__{account_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 's{account_id}' AS id, 'Squad from entity' AS name, "
        f"'c{account_id}' AS campaign_id, 'PAUSED' AS status, CAST(2500000 AS BIGINT) AS lifetime_budget_micro, "
        "TIMESTAMPTZ '2026-10-01 07:00:00+00' AS start_time, TIMESTAMPTZ '2026-10-31 22:59:59+00' AS end_time, "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00' AS created_at, TIMESTAMPTZ '2026-09-02 08:00:00+00' AS updated_at "
        f"WHERE {_when(ad_squads_entity)}"
    )
    client.execute(
        f"CREATE TABLE snapchat_ads.ads__{account_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 'a{account_id}' AS id, 'Ad from entity' AS name, "
        f"'s{account_id}' AS ad_squad_id, 'ACTIVE' AS status, "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00' AS created_at, TIMESTAMPTZ '2026-09-02 08:00:00+00' AS updated_at "
        f"WHERE {_when(ads_entity)}"
    )
    client.execute(
        f"CREATE TABLE snapchat_ads.campaigns_stats__{account_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 'c{account_id}', 100, 10, 12500000, 2, 40000000)"
        + (f", (DATE '2026-10-01', 'c{account_id}', 50, 5, 2500000, 1, 10000000)" if two_campaign_rows else "")
        + (", (DATE '2026-10-01', 'c999', 1, 1, 1000000, 0, 0)" if extra_fact_campaign else "")
        + ") AS t(date, id, impressions, swipes, spend, conversion_purchases, conversion_purchases_value)"
    )
    client.execute(
        f"CREATE TABLE snapchat_ads.ads_stats__{account_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 'a{account_id}', 100, 10, 12500000, 2, 40000000), "
        "(DATE '2026-10-01', 'a_unknown', 7, 1, 500000, 0, 0)"
        ") AS t(date, id, impressions, swipes, spend, conversion_purchases, conversion_purchases_value)"
    )


def _materialize(warehouse, *accounts: il.Source) -> il.Source:
    star = SnapchatAdsStarSchema(destinations=[warehouse])
    il.DAG(*accounts, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "snapchat_ads", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    @pytest.mark.parametrize(
        ("view", "name", "key"),
        [
            ("fact_campaign_performance", "campaigns_stats", "snapchat_ads.campaigns_stats"),
            ("fact_campaign_performance", "ad_accounts", "snapchat_ads.ad_accounts"),
            ("fact_ad_performance", "ads_stats", "snapchat_ads.ads_stats"),
            ("fact_ad_performance", "ad_accounts", "snapchat_ads.ad_accounts"),
            ("dim_accounts", "ad_accounts", "snapchat_ads.ad_accounts"),
            ("dim_campaigns", "campaigns", "snapchat_ads.campaigns"),
            ("dim_ad_groups", "ad_squads", "snapchat_ads.ad_squads"),
            ("dim_ads", "ads", "snapchat_ads.ads"),
        ],
    )
    def test_raw_upstreams_fan_in_every_account_and_are_optional(self, view: str, name: str, key: str):
        relation = getattr(SnapchatAdsStarSchema, view).relations[name]
        assert (relation.key, relation.many, relation.optional) == (key, True, True)

    def test_dimensions_read_the_facts_and_the_account_timezones(self):
        assert SnapchatAdsStarSchema.sibling_bindings() == {
            "dim_accounts": {"fact_campaign_performance": "fact_campaign_performance"},
            "dim_campaigns": {"fact_campaign_performance": "fact_campaign_performance", "dim_accounts": "dim_accounts"},
            "dim_ad_groups": {"dim_accounts": "dim_accounts"},
            "dim_ads": {"fact_ad_performance": "fact_ad_performance", "dim_ad_groups": "dim_ad_groups"},
        }

    def test_views_carry_the_core_columns_first(self):
        assert list(schemas.FactCampaignPerformance.model_fields) == FACT_CORE
        assert list(schemas.DimAccounts.model_fields) == [
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
        assert list(schemas.DimAds.model_fields) == [
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
        for asset in SnapchatAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert SnapchatAdsStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert SnapchatAdsStarSchema.dim_ad_groups.tags == ["Dimension"]

    def test_the_source_is_shown_under_snapchat_ads_in_its_dataset(self):
        assert SnapchatAdsStarSchema.group == "snapchat_ads"
        assert SnapchatAdsStarSchema().fact_campaign_performance.dataset == "snapchat_ads"


class TestStamping:
    def test_an_upstream_of_a_source_without_a_discriminator_is_refused(self):
        upstream = il.Upstream(asset=SnapchatAdsStarSchema().fact_campaign_performance, data=None)
        with pytest.raises(ConfigError, match="belongs to no account"):
            star_schema._account(upstream)


class TestOnDuckDB:
    def test_the_campaign_fact_sums_to_one_row_per_campaign_and_day_in_currency_units(self, warehouse):
        first, second = _account("aaa", warehouse), _account("bbb", warehouse)
        _seed(warehouse, "aaa", two_campaign_rows=True)
        _seed(warehouse, "bbb")
        _materialize(warehouse, first, second)
        rows = _rows(warehouse, "fact_campaign_performance")
        assert len(rows) == len({(row["date"], row["account_id"], row["campaign_id"]) for row in rows}) == 2
        [row] = [row for row in rows if row["account_id"] == "aaa"]
        assert (row["date"], row["campaign_id"]) == (DAY, "caaa")
        assert (row["impressions"], row["clicks"], row["cost"]) == (150, 15, 15.0)
        assert (row["purchases"], row["revenue"]) == (3, 50.0)
        assert row["conversions"] is None and row["conversion_value"] is None

    def test_the_currency_is_the_accounts_latest(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        assert {row["currency"] for row in _rows(warehouse, "fact_campaign_performance")} == {"EUR"}
        assert {row["currency"] for row in _rows(warehouse, "fact_ad_performance")} == {"EUR"}

    def test_without_an_account_snapshot_the_currency_is_null(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa", accounts_entity=False)
        _materialize(warehouse, first)
        assert {row["currency"] for row in _rows(warehouse, "fact_campaign_performance")} == {None}

    def test_with_no_account_snapshot_bound_the_currency_is_null(self, warehouse):
        first = _account("aaa", warehouse)
        first.assets = [asset for asset in first.assets if type(asset).key != "ad_accounts"]
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        assert {row["currency"] for row in _rows(warehouse, "fact_campaign_performance")} == {None}
        assert {row["currency"] for row in _rows(warehouse, "fact_ad_performance")} == {None}

    def test_the_ad_fact_keeps_one_row_per_ad_and_day(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        frame = warehouse.select("fact_ad_performance", "snapchat_ads", None)
        assert list(frame.columns) == list(schemas.FactAdPerformance.model_fields)
        rows = {row["ad_id"]: row for row in _rows(warehouse, "fact_ad_performance")}
        assert (rows["aaaa"]["clicks"], rows["aaaa"]["cost"], rows["aaaa"]["revenue"]) == (10, 12.5, 40.0)
        assert rows["a_unknown"]["cost"] == 0.5

    def test_an_account_takes_its_latest_snapshot_with_its_timezone(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {
                "account_id": "aaa",
                "account_name": "Acct aaa",
                "currency": "EUR",
                "timezone": "Europe/Berlin",
                "as_of_date": dt.date(2026, 9, 30),
            }
        ]

    def test_an_account_without_a_snapshot_comes_from_the_fact(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa", accounts_entity=False)
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "aaa", "account_name": None, "currency": None, "timezone": None, "as_of_date": DAY}
        ]

    def test_a_campaign_takes_its_entity_with_its_schedule_in_the_accounts_days(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert (row["campaign_id"], row["account_id"], row["campaign_name"], row["status"]) == (
            "caaa",
            "aaa",
            "Camp from entity",
            "ACTIVE",
        )
        assert (row["objective"], row["campaign_type"]) == ("WEB_CONVERSION", "AUCTION")
        assert (row["budget"], row["budget_type"]) == (17500.0, "lifetime")
        # 22:00 UTC on 30 September is midnight on 1 October in Berlin
        assert (row["start_date"], row["end_date"]) == (dt.date(2026, 10, 1), None)
        assert row["created_at"] == dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.timezone.utc)
        assert row["as_of_date"] == dt.date(2026, 9, 30)

    def test_the_schedule_is_read_in_each_accounts_own_timezone(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa", timezone="America/Los_Angeles")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert row["start_date"] == dt.date(2026, 9, 30)

    def test_without_the_accounts_timezone_the_schedule_is_null(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa", accounts_entity=False)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert row["campaign_name"] == "Camp from entity"
        assert (row["start_date"], row["end_date"]) == (None, None)

    def test_a_campaign_known_only_from_the_facts_comes_from_them(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa", extra_fact_campaign=True)
        _materialize(warehouse, first)
        rows = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["c999"]["account_id"] == "aaa"
        assert rows["c999"]["campaign_name"] is None and rows["c999"]["as_of_date"] == DAY
        assert rows["caaa"]["campaign_name"] == "Camp from entity"

    def test_ad_groups_take_the_ad_squads_entity(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_ad_groups")
        assert (row["ad_group_id"], row["campaign_id"], row["account_id"], row["status"]) == (
            "saaa",
            "caaa",
            "aaa",
            "PAUSED",
        )
        assert (row["budget"], row["budget_type"]) == (2.5, "lifetime")
        assert (row["start_date"], row["end_date"]) == (dt.date(2026, 10, 1), dt.date(2026, 10, 31))

    def test_an_ad_takes_its_entity_and_its_campaign_through_its_ad_squad(self, warehouse):
        first = _account("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        rows = {row["ad_id"]: row for row in _rows(warehouse, "dim_ads")}
        assert (rows["aaaa"]["ad_group_id"], rows["aaaa"]["campaign_id"], rows["aaaa"]["ad_name"]) == (
            "saaa",
            "caaa",
            "Ad from entity",
        )
        unknown = rows["a_unknown"]
        assert (unknown["account_id"], unknown["ad_group_id"], unknown["campaign_id"]) == ("aaa", None, None)
        assert unknown["as_of_date"] == DAY

    def test_every_key_a_fact_carries_resolves_in_its_dimension(self, warehouse):
        first, second = _account("aaa", warehouse), _account("bbb", warehouse)
        _seed(warehouse, "aaa", extra_fact_campaign=True)
        _seed(warehouse, "bbb", campaigns_entity=False, ads_entity=False)
        _materialize(warehouse, first, second)
        assert {row["campaign_id"] for row in _rows(warehouse, "fact_campaign_performance")} <= {
            row["campaign_id"] for row in _rows(warehouse, "dim_campaigns")
        }
        assert {row["ad_id"] for row in _rows(warehouse, "fact_ad_performance")} <= {
            row["ad_id"] for row in _rows(warehouse, "dim_ads")
        }
        assert {row["account_id"] for row in _rows(warehouse, "dim_accounts")} == {"aaa", "bbb"}

    def test_a_missing_account_table_fails_the_view(self, warehouse):
        first = _account("aaa", warehouse)
        star = SnapchatAdsStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "campaigns_stats__aaa" in error

    def test_a_fact_with_no_account_bound_fails_clearly(self, warehouse):
        star = SnapchatAdsStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
