"""Tests for the Criteo star schema: views over every advertiser's report and entity tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError
from interloper.runner import ExecutionStatus

from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.connector.source import Criteo
from interloper_assets.criteo.star_schema import schemas
from interloper_assets.criteo.star_schema import source as star_schema
from interloper_assets.criteo.star_schema.source import CriteoStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)
FACT_COLUMNS = [
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
MEASURES = "displays, clicks, advertiser_cost, currency, sales_client_attribution, revenue_generated_client_attribution"
SNAPSHOT_DAY = dt.date(2026, 9, 30)
ENTITY_COLUMNS = {
    "advertisers": "date DATE, id VARCHAR, type VARCHAR, meta VARCHAR, advertiser_name VARCHAR",
    "campaigns": "date DATE, id VARCHAR, type VARCHAR, advertiser_id VARCHAR, name VARCHAR, goal VARCHAR, "
    "spend_limit_spend_limit_type VARCHAR, spend_limit_spend_limit_renewal VARCHAR, "
    "spend_limit_spend_limit_amount_value DOUBLE",
    "ad_sets": "date DATE, id VARCHAR, campaign_id VARCHAR, name VARCHAR, schedule_activation_status VARCHAR, "
    "schedule_delivery_status VARCHAR, budget_budget_renewal VARCHAR, budget_budget_amount DOUBLE",
    "ads": "date DATE, id VARCHAR, ad_set_id VARCHAR, name VARCHAR",
}


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _advertiser(advertiser_id: str, warehouse) -> il.Source:
    connection = CriteoConnection(id="criteo", client_id="x", client_secret="x", refresh_token="x")
    return Criteo(advertiser_id=advertiser_id, connection=connection, destinations=[warehouse], enabled=False)


def _seed(
    warehouse,
    advertiser_id: str,
    *,
    campaign_rows: list[str],
    ad_rows: list[str],
    entities: dict[str, list[str]] | None = None,
    skip: tuple[str, ...] = (),
) -> None:
    """Create the advertiser's report and entity tables; an entity without rows is collected but empty."""
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS criteo")
    for key, columns in ENTITY_COLUMNS.items():
        if key in skip:
            continue
        client.execute(f"CREATE TABLE criteo.{key}__{advertiser_id} ({columns})")
        if entities and entities.get(key):
            client.execute(f"INSERT INTO criteo.{key}__{advertiser_id} VALUES {', '.join(entities[key])}")
    client.execute(
        f"CREATE TABLE criteo.campaigns_stats__{advertiser_id} AS SELECT * FROM (VALUES {', '.join(campaign_rows)}) "
        f"AS t(day, campaign_id, campaign, {MEASURES})"
    )
    client.execute(
        f"CREATE TABLE criteo.ads_stats__{advertiser_id} AS SELECT * FROM (VALUES {', '.join(ad_rows)}) "
        f"AS t(day, campaign_id, campaign, adset_id, adset, ad_id, ad, {MEASURES})"
    )


def _campaign_row(campaign_id: str, name: str, day: str = "2026-10-01", displays: int = 100) -> str:
    return f"(DATE '{day}', '{campaign_id}', '{name}', {displays}, 10, 12.5, 'EUR', 2, 40.0)"


def _ad_row(ad_id: str, name: str, adset_id: str = "s1", day: str = "2026-10-01") -> str:
    return (
        f"(DATE '{day}', 'c1', 'Camp', '{adset_id}', 'Set {adset_id}', '{ad_id}', '{name}', "
        "100, 10, 12.5, 'EUR', 2, 40.0)"
    )


def _seed_one(warehouse, advertiser_id: str, **kwargs) -> None:
    _seed(
        warehouse, advertiser_id, campaign_rows=[_campaign_row("c1", "Camp")], ad_rows=[_ad_row("a1", "Ad")], **kwargs
    )


def _snapshot(advertiser_id: str, day: str = "2026-09-30") -> dict[str, list[str]]:
    """One snapshot of the advertiser and the campaign, ad set and ads its reports name, plus an ad they do not.

    Returns:
        The rows of each entity table, as SQL value tuples.
    """
    return {
        "advertisers": [f"(DATE '{day}', '{advertiser_id}', 'Advertiser', NULL, 'Advertiser {advertiser_id}')"],
        "campaigns": [
            (
                f"(DATE '{day}', 'c1', 'Campaign', '{advertiser_id}', 'Camp entity', 'acquisition', 'capped', "
                "'monthly', 900.0)"
            )
        ],
        "ad_sets": [f"(DATE '{day}', 's1', 'c1', 'Set entity', 'on', 'live', 'daily', 40.0)"],
        "ads": [f"(DATE '{day}', 'a1', 's1', 'Ad entity')", f"(DATE '{day}', 'a9', 's1', 'Unreported ad')"],
    }


def _materialize(warehouse, *advertisers: il.Source) -> il.Source:
    star = CriteoStarSchema(destinations=[warehouse])
    il.DAG(*advertisers, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "criteo", None)
    for column in ("date", "as_of_date", "start_date", "end_date"):
        if column in frame:
            frame[column] = frame[column].dt.date
    frame = frame.sort_values(list(frame.columns))
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


class TestWiring:
    @pytest.mark.parametrize(
        ("view", "name", "key"),
        [
            ("fact_campaign_performance", "campaigns_stats", "criteo.campaigns_stats"),
            ("fact_ad_performance", "ads_stats", "criteo.ads_stats"),
            ("dim_campaigns", "campaigns_stats", "criteo.campaigns_stats"),
            ("dim_ad_groups", "ads_stats", "criteo.ads_stats"),
            ("dim_ads", "ads_stats", "criteo.ads_stats"),
        ],
    )
    def test_raw_upstreams_fan_in_every_advertiser_and_are_optional(self, view: str, name: str, key: str):
        relation = getattr(CriteoStarSchema, view).relations[name]
        assert (relation.key, relation.many, relation.optional) == (key, True, True)

    @pytest.mark.parametrize(
        ("view", "entity"),
        [
            ("dim_accounts", "advertisers"),
            ("dim_campaigns", "campaigns"),
            ("dim_ad_groups", "ad_sets"),
            ("dim_ads", "ads"),
        ],
    )
    def test_each_dimension_fans_in_its_entity_and_is_optional(self, view: str, entity: str):
        relation = getattr(CriteoStarSchema, view).relations[entity]
        assert (relation.key, relation.many, relation.optional) == (f"criteo.{entity}", True, True)

    def test_dimensions_fall_back_to_the_facts(self):
        assert CriteoStarSchema.sibling_bindings() == {
            "dim_accounts": {"fact_campaign_performance": "fact_campaign_performance"},
            "dim_campaigns": {"fact_campaign_performance": "fact_campaign_performance"},
            "dim_ad_groups": {"fact_ad_performance": "fact_ad_performance"},
            "dim_ads": {"fact_ad_performance": "fact_ad_performance", "dim_ad_groups": "dim_ad_groups"},
        }

    def test_views_publish_only_what_criteo_fills(self):
        assert list(schemas.FactCampaignPerformance.model_fields) == FACT_COLUMNS
        assert list(schemas.DimAccounts.model_fields) == ["account_id", "account_name", "currency", "as_of_date"]
        assert list(schemas.DimCampaigns.model_fields) == [
            "campaign_id",
            "account_id",
            "campaign_name",
            "objective",
            "budget",
            "budget_type",
            "as_of_date",
        ]
        assert list(schemas.DimAdGroups.model_fields) == [
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_group_name",
            "status",
            "budget",
            "budget_type",
            "as_of_date",
            "effective_status",
        ]
        assert list(schemas.DimAds.model_fields) == [
            "ad_id",
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_name",
            "as_of_date",
        ]

    def test_views_are_unpartitioned_and_tagged(self):
        for asset in CriteoStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert CriteoStarSchema.fact_campaign_performance.tags == ["Fact"]
        assert CriteoStarSchema.dim_ads.tags == ["Dimension"]


class TestStamping:
    def test_an_upstream_of_a_source_without_a_discriminator_is_refused(self):
        upstream = il.Upstream(asset=CriteoStarSchema().fact_campaign_performance, data=None)
        with pytest.raises(ConfigError, match="belongs to no account"):
            star_schema._account(upstream)


class TestOnDuckDB:
    def test_the_campaign_fact_sums_to_one_row_per_campaign_and_day(self, warehouse):
        first, second = _advertiser("111", warehouse), _advertiser("222", warehouse)
        rows = [_campaign_row("c1", "Camp"), _campaign_row("c1", "Camp", displays=50)]
        _seed(warehouse, "111", campaign_rows=rows, ad_rows=[_ad_row("a1", "Ad")])
        _seed_one(warehouse, "222")
        _materialize(warehouse, first, second)
        facts = _rows(warehouse, "fact_campaign_performance")
        assert len(facts) == len({(row["date"], row["account_id"], row["campaign_id"]) for row in facts}) == 2
        [row] = [row for row in facts if row["account_id"] == "111"]
        assert (row["date"], row["campaign_id"], row["impressions"], row["clicks"]) == (DAY, "c1", 150, 20)
        assert (row["cost"], row["currency"]) == (25.0, "EUR")
        assert (row["conversions"], row["conversion_value"], row["purchases"], row["revenue"]) == (4, 80.0, 4, 80.0)

    def test_the_ad_fact_keeps_the_ads_keys(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed_one(warehouse, "111")
        _materialize(warehouse, first)
        frame = warehouse.select("fact_ad_performance", "criteo", None)
        assert list(frame.columns) == list(schemas.FactAdPerformance.model_fields)
        [row] = _rows(warehouse, "fact_ad_performance")
        assert (row["account_id"], row["campaign_id"], row["ad_group_id"], row["ad_id"]) == ("111", "c1", "s1", "a1")
        assert (row["impressions"], row["cost"], row["purchases"]) == (100, 12.5, 2)

    def test_an_advertiser_takes_the_currency_of_its_reports(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed_one(warehouse, "111")
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "111", "account_name": None, "currency": "EUR", "as_of_date": DAY}
        ]

    def test_a_campaign_takes_the_name_of_its_latest_report(self, warehouse):
        first = _advertiser("111", warehouse)
        rows = [_campaign_row("c1", "Old name", day="2026-09-30"), _campaign_row("c1", "New name")]
        _seed(warehouse, "111", campaign_rows=rows, ad_rows=[_ad_row("a1", "Ad")])
        _materialize(warehouse, first)
        [row] = _rows(warehouse, "dim_campaigns")
        assert (row["campaign_id"], row["account_id"], row["campaign_name"], row["as_of_date"]) == (
            "c1",
            "111",
            "New name",
            DAY,
        )

    def test_tied_report_rows_collapse_deterministically(self, warehouse):
        first = _advertiser("111", warehouse)
        rows = [_campaign_row("c1", "Name A"), _campaign_row("c1", "Name B")]
        _seed(warehouse, "111", campaign_rows=rows, ad_rows=[_ad_row("a1", "Ad")])
        _materialize(warehouse, first)
        names = {_rows(warehouse, "dim_campaigns")[0]["campaign_name"] for _ in range(5)}
        assert names == {"Name B"}

    def test_ad_sets_and_ads_take_their_names_and_parents_from_the_ad_report(self, warehouse):
        first = _advertiser("111", warehouse)
        ads = [_ad_row("a1", "First"), _ad_row("a2", "Second", adset_id="s2"), _ad_row("a1", "Old", day="2026-09-30")]
        _seed(warehouse, "111", campaign_rows=[_campaign_row("c1", "Camp")], ad_rows=ads)
        _materialize(warehouse, first)
        groups = {row["ad_group_id"]: row for row in _rows(warehouse, "dim_ad_groups")}
        assert (groups["s2"]["ad_group_name"], groups["s2"]["campaign_id"], groups["s2"]["account_id"]) == (
            "Set s2",
            "c1",
            "111",
        )
        ads_rows = {row["ad_id"]: row for row in _rows(warehouse, "dim_ads")}
        assert (ads_rows["a1"]["ad_name"], ads_rows["a1"]["ad_group_id"], ads_rows["a1"]["campaign_id"]) == (
            "First",
            "s1",
            "c1",
        )
        assert ads_rows["a2"]["ad_group_id"] == "s2"

    def test_every_key_a_fact_carries_resolves_in_its_dimension(self, warehouse):
        first, second = _advertiser("111", warehouse), _advertiser("222", warehouse)
        _seed_one(warehouse, "111")
        _seed(warehouse, "222", campaign_rows=[_campaign_row("c9", "Other")], ad_rows=[_ad_row("a9", "Other")])
        _materialize(warehouse, first, second)
        assert {(row["account_id"], row["campaign_id"]) for row in _rows(warehouse, "fact_campaign_performance")} <= {
            (row["account_id"], row["campaign_id"]) for row in _rows(warehouse, "dim_campaigns")
        }
        assert {row["ad_id"] for row in _rows(warehouse, "fact_ad_performance")} <= {
            row["ad_id"] for row in _rows(warehouse, "dim_ads")
        }
        assert {row["ad_group_id"] for row in _rows(warehouse, "fact_ad_performance")} <= {
            row["ad_group_id"] for row in _rows(warehouse, "dim_ad_groups")
        }
        assert {row["account_id"] for row in _rows(warehouse, "dim_accounts")} == {"111", "222"}

    def test_a_row_without_a_key_reaches_no_view(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed_one(warehouse, "111")
        client = warehouse.connection.client
        client.execute(
            "INSERT INTO criteo.campaigns_stats__111 SELECT * REPLACE (NULL AS campaign_id) "
            f"FROM (VALUES {_campaign_row('c2', 'Camp')}) AS t(day, campaign_id, campaign, {MEASURES})"
        )
        client.execute(
            "INSERT INTO criteo.ads_stats__111 "
            "SELECT * REPLACE (NULL AS campaign_id, NULL AS adset_id, NULL AS ad_id) "
            f"FROM (VALUES {_ad_row('a2', 'Ad')}) "
            f"AS t(day, campaign_id, campaign, adset_id, adset, ad_id, ad, {MEASURES})"
        )
        _materialize(warehouse, first)
        keys = {
            ("fact_campaign_performance", "campaign_id"): {"c1"},
            ("fact_ad_performance", "ad_id"): {"a1"},
            ("dim_campaigns", "campaign_id"): {"c1"},
            ("dim_ad_groups", "ad_group_id"): {"s1"},
            ("dim_ads", "ad_id"): {"a1"},
        }
        for (table, key), expected in keys.items():
            assert {row[key] for row in _rows(warehouse, table)} == expected

    def test_a_missing_advertiser_table_fails_the_view(self, warehouse):
        first = _advertiser("111", warehouse)
        star = CriteoStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "campaigns_stats__111" in error

    def test_an_advertiser_takes_its_name_from_its_snapshot_and_its_currency_from_its_reports(self, warehouse):
        first, second = _advertiser("111", warehouse), _advertiser("222", warehouse)
        _seed_one(warehouse, "111", entities=_snapshot("111"))
        _seed_one(warehouse, "222")
        _materialize(warehouse, first, second)
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "111", "account_name": "Advertiser 111", "currency": "EUR", "as_of_date": SNAPSHOT_DAY},
            {"account_id": "222", "account_name": None, "currency": "EUR", "as_of_date": DAY},
        ]

    def test_a_campaign_takes_its_snapshot_over_a_later_report(self, warehouse):
        first = _advertiser("111", warehouse)
        rows = [_campaign_row("c1", "Camp"), _campaign_row("c2", "Reported only")]
        _seed(warehouse, "111", campaign_rows=rows, ad_rows=[_ad_row("a1", "Ad")], entities=_snapshot("111"))
        _materialize(warehouse, first)
        campaigns = {row["campaign_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert campaigns["c1"] == {
            "campaign_id": "c1",
            "account_id": "111",
            "campaign_name": "Camp entity",
            "objective": "acquisition",
            "budget": 900.0,
            "budget_type": "monthly",
            "as_of_date": SNAPSHOT_DAY,
        }
        assert campaigns["c2"] == {
            "campaign_id": "c2",
            "account_id": "111",
            "campaign_name": "Reported only",
            "objective": None,
            "budget": None,
            "budget_type": None,
            "as_of_date": DAY,
        }

    def test_an_ad_set_takes_its_status_and_budget_from_its_snapshot(self, warehouse):
        first = _advertiser("111", warehouse)
        ads = [_ad_row("a1", "Ad"), _ad_row("a2", "Other", adset_id="s2")]
        _seed(warehouse, "111", campaign_rows=[_campaign_row("c1", "Camp")], ad_rows=ads, entities=_snapshot("111"))
        _materialize(warehouse, first)
        groups = {row["ad_group_id"]: row for row in _rows(warehouse, "dim_ad_groups")}
        assert groups["s1"] == {
            "ad_group_id": "s1",
            "campaign_id": "c1",
            "account_id": "111",
            "ad_group_name": "Set entity",
            "status": "on",
            "budget": 40.0,
            "budget_type": "daily",
            "as_of_date": SNAPSHOT_DAY,
            "effective_status": "live",
        }
        assert (groups["s2"]["ad_group_name"], groups["s2"]["status"], groups["s2"]["as_of_date"]) == (
            "Set s2",
            None,
            DAY,
        )

    def test_an_ad_takes_its_campaign_from_its_ad_set(self, warehouse):
        first = _advertiser("111", warehouse)
        _seed_one(warehouse, "111", entities=_snapshot("111"))
        _materialize(warehouse, first)
        ads = {row["ad_id"]: row for row in _rows(warehouse, "dim_ads")}
        assert ads["a1"]["ad_name"] == "Ad entity"
        assert ads["a9"] == {
            "ad_id": "a9",
            "ad_group_id": "s1",
            "campaign_id": "c1",
            "account_id": "111",
            "ad_name": "Unreported ad",
            "as_of_date": SNAPSHOT_DAY,
        }

    def test_the_latest_snapshot_wins(self, warehouse):
        first = _advertiser("111", warehouse)
        older, newer = _snapshot("111", "2026-09-28"), _snapshot("111", "2026-09-30")
        newer["campaigns"] = [row.replace("Camp entity", "Camp renamed") for row in newer["campaigns"]]
        _seed_one(warehouse, "111", entities={key: older[key] + newer[key] for key in older})
        _materialize(warehouse, first)
        assert {_rows(warehouse, "dim_campaigns")[0]["campaign_name"] for _ in range(5)} == {"Camp renamed"}

    def test_a_snapshot_row_without_an_id_reaches_no_view(self, warehouse):
        first = _advertiser("111", warehouse)
        entities = _snapshot("111")
        entities["campaigns"].append("(DATE '2026-09-30', NULL, 'Campaign', '111', 'Orphan', NULL, NULL, NULL, NULL)")
        entities["ad_sets"].append("(DATE '2026-09-30', NULL, 'c1', 'Orphan', 'on', 'live', NULL, NULL)")
        entities["ads"].append("(DATE '2026-09-30', NULL, 's1', 'Orphan')")
        _seed_one(warehouse, "111", entities=entities)
        _materialize(warehouse, first)
        assert {row["campaign_id"] for row in _rows(warehouse, "dim_campaigns")} == {"c1"}
        assert {row["ad_group_id"] for row in _rows(warehouse, "dim_ad_groups")} == {"s1"}
        assert {row["ad_id"] for row in _rows(warehouse, "dim_ads")} == {"a1", "a9"}

    @pytest.mark.parametrize(
        ("entity", "view"),
        [("advertisers", "dim_accounts"), ("campaigns", "dim_campaigns"), ("ad_sets", "dim_ad_groups")],
    )
    def test_a_missing_entity_table_fails_the_dimension(self, warehouse, entity: str, view: str):
        first = _advertiser("111", warehouse)
        _seed_one(warehouse, "111", skip=(entity,))
        star = CriteoStarSchema(destinations=[warehouse])
        result = il.DAG(first, star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[getattr(star, view).id].error
        assert error is not None and f"{entity}__111" in error

    def test_a_fact_with_no_advertiser_bound_fails_clearly(self, warehouse):
        star = CriteoStarSchema(destinations=[warehouse])
        result = il.DAG(star).materialize()
        assert result.status is ExecutionStatus.FAILED
        error = result.executions[star.fact_campaign_performance.id].error
        assert error is not None and "no upstream bound" in error
