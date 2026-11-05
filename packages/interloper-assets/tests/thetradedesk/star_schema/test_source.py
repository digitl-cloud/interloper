"""Tests for The Trade Desk star schema: views over every partner's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_assets.thetradedesk.connection import TheTradeDeskConnection
from interloper_assets.thetradedesk.connector.source import TheTradeDesk
from interloper_assets.thetradedesk.star_schema import schemas
from interloper_assets.thetradedesk.star_schema import source as star_schema
from interloper_assets.thetradedesk.star_schema.source import TheTradeDeskStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)
REPORT_COLUMNS = (
    "date, advertiser_id, campaign_id, ad_group_id, campaign, ad_group, creative_id, impressions, clicks, "
    "advertiser_cost_adv_currency, advertiser_currency_code, partner_currency_code"
)


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _partner(partner_id: str, warehouse) -> il.Source:
    connection = TheTradeDeskConnection(id="ttd", api_key="x")
    return TheTradeDesk(partner_id=partner_id, connection=connection, destinations=[warehouse], enabled=False)


def _when(collected: bool) -> str:
    return "TRUE" if collected else "FALSE"


def _seed(
    warehouse,
    partner_id: str,
    *,
    campaigns_entity: bool = True,
    ad_groups_entity: bool = True,
    extra_report_campaign: bool = False,
) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS the_trade_desk")
    # two creatives of one ad group, a second ad group of the same campaign, all one day
    client.execute(
        f"CREATE TABLE the_trade_desk.ad_groups_stats__{partner_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-10-01', 'adv{partner_id}', 'c{partner_id}', 'g{partner_id}', 'Camp from report', "
        f"'Group from report', 'cr1', 100, 10, 12.5, 'CHF', 'EUR'), "
        f"(DATE '2026-10-01', 'adv{partner_id}', 'c{partner_id}', 'g{partner_id}', 'Camp from report', "
        f"'Group from report', 'cr2', 50, 5, 2.5, 'CHF', 'EUR'), "
        f"(DATE '2026-10-01', 'adv{partner_id}', 'c{partner_id}', 'h{partner_id}', 'Camp from report', "
        f"'Second group', 'cr3', 20, 2, 1.0, 'CHF', 'EUR')"
        + (
            ", (DATE '2026-10-01', 'adv9', 'c999', 'g999', 'Only in report', 'Only group', 'cr9', 1, 1, 1.0, "
            "'USD', 'EUR')"
            if extra_report_campaign
            else ""
        )
        + f") AS t({REPORT_COLUMNS})"
    )
    client.execute(
        f"CREATE TABLE the_trade_desk.campaigns__{partner_id} AS SELECT * FROM (VALUES "
        f"(DATE '2026-09-29', 'c{partner_id}', 'Old name', 'adv{partner_id}', 'Paused', 'CPA', 'Standard', "
        "CAST(500 AS DOUBLE), TIMESTAMPTZ '2026-09-30 22:00:00+00', CAST(NULL AS TIMESTAMPTZ), "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00'), "
        f"(DATE '2026-09-30', 'c{partner_id}', 'Camp from entity', 'adv{partner_id}', 'Active', 'CPA', 'Standard', "
        "CAST(1000 AS DOUBLE), TIMESTAMPTZ '2026-09-30 22:00:00+00', TIMESTAMPTZ '2026-10-31 22:59:59+00', "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00')"
        ") AS t(date, id, name, advertiser_id, status, primary_goal_type, type, budget_total, start_date, end_date, "
        f"created_at_utc) WHERE {_when(campaigns_entity)}"
    )
    client.execute(
        f"CREATE TABLE the_trade_desk.ad_groups__{partner_id} AS SELECT "
        f"DATE '2026-09-30' AS date, 'g{partner_id}' AS id, 'Group from entity' AS name, "
        f"'c{partner_id}' AS campaign_id, 'Active' AS status, CAST(250 AS DOUBLE) AS budget_in_advertiser_currency, "
        "TIMESTAMPTZ '2026-09-01 08:00:00+00' AS created_at, TIMESTAMPTZ '2026-09-02 08:00:00+00' AS last_updated "
        f"WHERE {_when(ad_groups_entity)}"
    )


def _materialize(warehouse, *partners: il.Source) -> il.Source:
    star = TheTradeDeskStarSchema(destinations=[warehouse])
    il.DAG(*partners, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "the_trade_desk", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    @pytest.mark.parametrize(
        ("view", "name", "key"),
        [
            ("fact_campaign_performance", "ad_groups_stats", "the_trade_desk.ad_groups_stats"),
            ("fact_ad_group_performance", "ad_groups_stats", "the_trade_desk.ad_groups_stats"),
            ("dim_accounts", "ad_groups_stats", "the_trade_desk.ad_groups_stats"),
            ("dim_campaigns", "campaigns", "the_trade_desk.campaigns"),
            ("dim_campaigns", "ad_groups_stats", "the_trade_desk.ad_groups_stats"),
            ("dim_ad_groups", "ad_groups", "the_trade_desk.ad_groups"),
            ("dim_ad_groups", "ad_groups_stats", "the_trade_desk.ad_groups_stats"),
        ],
    )
    def test_raw_upstreams_fan_in_every_partner_and_are_optional(self, view: str, name: str, key: str):
        relation = getattr(TheTradeDeskStarSchema, view).relations[name]
        assert (relation.key, relation.many, relation.optional) == (key, True, True)

    def test_dimensions_fall_back_to_their_facts(self):
        assert TheTradeDeskStarSchema.sibling_bindings() == {
            "dim_campaigns": {"fact_campaign_performance": "fact_campaign_performance"},
            "dim_ad_groups": {"fact_ad_group_performance": "fact_ad_group_performance"},
        }

    def test_views_omit_what_the_trade_desk_does_not_fill(self):
        for column in ("conversions", "conversion_value", "purchases", "revenue"):
            assert column not in schemas.FactCampaignPerformance.model_fields
        assert "account_name" not in schemas.DimAccounts.model_fields
        assert "updated_at" not in schemas.DimCampaigns.model_fields
        assert {"start_date", "end_date"}.isdisjoint(schemas.DimAdGroups.model_fields)

    def test_views_are_unpartitioned_and_tagged(self):
        for asset in TheTradeDeskStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert TheTradeDeskStarSchema.fact_ad_group_performance.tags == ["Fact"]
        assert TheTradeDeskStarSchema.dim_ad_groups.tags == ["Dimension"]

    def test_the_source_is_shown_under_the_trade_desk_in_its_dataset(self):
        assert TheTradeDeskStarSchema.group == "the_trade_desk"
        assert TheTradeDeskStarSchema().fact_campaign_performance.dataset == "the_trade_desk"


class TestStamping:
    def test_an_upstream_of_a_source_without_a_discriminator_is_refused(self):
        upstream = il.Upstream(asset=TheTradeDeskStarSchema().fact_campaign_performance, data=None)
        with pytest.raises(ConfigError, match="belongs to no account"):
            star_schema._account(upstream)


class TestOnDuckDB:
    def test_the_campaign_fact_sums_to_one_row_per_campaign_and_day(self, warehouse):
        first, second = _partner("aaa", warehouse), _partner("bbb", warehouse)
        _seed(warehouse, "aaa")
        _seed(warehouse, "bbb")
        _materialize(warehouse, first, second)
        frame = warehouse.select("fact_campaign_performance", "the_trade_desk", None)
        assert list(frame.columns) == list(schemas.FactCampaignPerformance.model_fields)
        rows = _rows(warehouse, "fact_campaign_performance")
        assert len(rows) == len({(row["date"], row["account_id"], row["campaign_id"]) for row in rows}) == 2
        [row] = [row for row in rows if row["account_id"] == "aaa"]
        assert (row["date"], row["campaign_id"]) == (DAY, "caaa")
        assert (row["impressions"], row["clicks"], row["cost"], row["currency"]) == (170, 17, 16.0, "CHF")

    def test_the_ad_group_fact_sums_creatives_to_one_row_per_ad_group_and_day(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        frame = warehouse.select("fact_ad_group_performance", "the_trade_desk", None)
        assert list(frame.columns) == list(schemas.FactAdGroupPerformance.model_fields)
        rows = {row["ad_group_id"]: row for row in _rows(warehouse, "fact_ad_group_performance")}
        assert set(rows) == {"gaaa", "haaa"}
        assert (rows["gaaa"]["campaign_id"], rows["gaaa"]["impressions"], rows["gaaa"]["cost"]) == ("caaa", 150, 15.0)
        assert (rows["haaa"]["clicks"], rows["haaa"]["currency"]) == (2, "CHF")

    def test_an_account_is_the_partner_with_its_own_currency_in_utc(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "aaa", "currency": "EUR", "timezone": "UTC", "as_of_date": DAY}
        ]

    def test_a_campaign_takes_its_latest_snapshot_with_its_schedule_as_utc_days(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert (row["campaign_id"], row["account_id"], row["advertiser_id"]) == ("caaa", "aaa", "advaaa")
        assert (row["campaign_name"], row["status"], row["objective"], row["campaign_type"]) == (
            "Camp from entity",
            "Active",
            "CPA",
            "Standard",
        )
        assert (row["budget"], row["budget_type"]) == (1000.0, "lifetime")
        # The Trade Desk reports in UTC, so 22:00 UTC on 30 September stays 30 September
        assert (row["start_date"], row["end_date"]) == (dt.date(2026, 9, 30), dt.date(2026, 10, 31))
        assert row["created_at"] == dt.datetime(2026, 9, 1, 8, 0, tzinfo=dt.timezone.utc)
        assert row["as_of_date"] == dt.date(2026, 9, 30)

    def test_without_a_snapshot_a_campaign_comes_from_the_report(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa", campaigns_entity=False)
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert (row["campaign_name"], row["advertiser_id"], row["as_of_date"]) == ("Camp from report", "advaaa", DAY)
        assert (row["status"], row["budget"], row["start_date"]) == (None, None, None)

    def test_with_no_snapshot_bound_the_dimensions_fall_through_to_the_report(self, warehouse):
        first = _partner("aaa", warehouse)
        first.assets = [asset for asset in first.assets if type(asset).key not in ("campaigns", "ad_groups")]
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        assert {row["campaign_name"] for row in _rows(warehouse, "dim_campaigns")} == {"Camp from report"}
        assert {row["ad_group_name"] for row in _rows(warehouse, "dim_ad_groups")} == {
            "Group from report",
            "Second group",
        }

    def test_an_ad_group_takes_its_snapshot_else_its_report(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa")
        _materialize(warehouse, first)
        rows = {row["ad_group_id"]: row for row in _rows(warehouse, "dim_ad_groups")}
        entity, report = rows["gaaa"], rows["haaa"]
        assert (entity["campaign_id"], entity["account_id"], entity["ad_group_name"], entity["status"]) == (
            "caaa",
            "aaa",
            "Group from entity",
            "Active",
        )
        assert (entity["budget"], entity["budget_type"]) == (250.0, "lifetime")
        assert entity["updated_at"] == dt.datetime(2026, 9, 2, 8, 0, tzinfo=dt.timezone.utc)
        assert (report["campaign_id"], report["ad_group_name"], report["status"], report["as_of_date"]) == (
            "caaa",
            "Second group",
            None,
            DAY,
        )

    def test_tied_report_rows_yield_the_same_row_on_every_evaluation(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa", campaigns_entity=False)
        warehouse.connection.client.execute(
            "INSERT INTO the_trade_desk.ad_groups_stats__aaa VALUES (DATE '2026-10-01', 'advaaa', 'caaa', 'gaaa', "
            "'A renamed campaign', 'Group from report', 'cr4', 1, 0, 0.0, 'CHF', 'EUR')"
        )
        _materialize(warehouse, first)
        names = {_rows(warehouse, "dim_campaigns")[0]["campaign_name"] for _ in range(5)}
        assert names == {"Camp from report"}

    def test_every_key_a_fact_carries_resolves_in_its_dimension(self, warehouse):
        first, second = _partner("aaa", warehouse), _partner("bbb", warehouse)
        _seed(warehouse, "aaa", extra_report_campaign=True)
        _seed(warehouse, "bbb", campaigns_entity=False, ad_groups_entity=False)
        _materialize(warehouse, first, second)
        assert {row["campaign_id"] for row in _rows(warehouse, "fact_campaign_performance")} <= {
            row["campaign_id"] for row in _rows(warehouse, "dim_campaigns")
        }
        assert {row["ad_group_id"] for row in _rows(warehouse, "fact_ad_group_performance")} <= {
            row["ad_group_id"] for row in _rows(warehouse, "dim_ad_groups")
        }
        assert {row["account_id"] for row in _rows(warehouse, "dim_accounts")} == {"aaa", "bbb"}

    def test_a_row_without_a_key_reaches_no_view(self, warehouse):
        first = _partner("aaa", warehouse)
        _seed(warehouse, "aaa")
        client = warehouse.connection.client
        client.execute(
            "INSERT INTO the_trade_desk.ad_groups_stats__aaa VALUES "
            "(DATE '2026-10-01', 'advaaa', NULL, NULL, NULL, NULL, 'cr5', 100, 10, 5.0, 'CHF', 'EUR')"
        )
        client.execute(
            "INSERT INTO the_trade_desk.ad_groups__aaa VALUES (DATE '2026-09-30', NULL, 'No id', 'caaa', 'Active', "
            "NULL, NULL, NULL)"
        )
        _materialize(warehouse, first)
        keys = {
            ("fact_campaign_performance", "campaign_id"): {"caaa"},
            ("fact_ad_group_performance", "ad_group_id"): {"gaaa", "haaa"},
            ("dim_campaigns", "campaign_id"): {"caaa"},
            ("dim_ad_groups", "ad_group_id"): {"gaaa", "haaa"},
        }
        for (table, key), expected in keys.items():
            assert {row[key] for row in _rows(warehouse, table)} == expected
        [row] = _rows(warehouse, "fact_campaign_performance")
        assert row["impressions"] == 170

    def test_a_missing_partner_table_fails_the_view(self, warehouse):
        first = _partner("aaa", warehouse)
        star = TheTradeDeskStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "ad_groups_stats__aaa" in error

    def test_a_fact_with_no_partner_bound_fails_clearly(self, warehouse):
        star = TheTradeDeskStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
