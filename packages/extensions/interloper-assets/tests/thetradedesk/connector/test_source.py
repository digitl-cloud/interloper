"""Regression tests for the TheTradeDesk source.

MyReports are requested in "International" format: metric values carry decimal
commas ("12,34"), the date column is DD/MM/YYYY, and quartile headers spell
``%`` ("Player 25% Complete" → ``player_25pct_complete``). The custom
normalizer must convert all three so the generic casts succeed — and survive
the host→child DAG-spec round-trip.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.asset import Asset
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData

from interloper_assets.thetradedesk import constants
from interloper_assets.thetradedesk.connection import TheTradeDeskConnection
from interloper_assets.thetradedesk.connector import schemas
from interloper_assets.thetradedesk.connector.source import TheTradeDesk, TheTradeDeskNormalizer

_ASSET_KEY = "ad_groups_stats"
ENTITY_ASSETS = ("advertisers", "campaigns", "ad_groups")
DAY = dt.date(2026, 10, 1)

# A CSV row as csv.DictReader yields it: every value a string, international format.
_ROW = {
    "Date": "10/07/2026",
    "Partner ID": "qz94j5z",
    "Ad Group": "AG, One",
    "Impressions": "1000",
    "TTD Cost (USD)": "12,34",
    "Player 25% Complete": "7",
    "Player 50% Complete": "4",
    "Sampled Tracked Impressions": "",
}


def _source() -> Any:
    return TheTradeDesk(id="src-1", partner_id="qz94j5z", connection=TheTradeDeskConnection(api_key="x"))


def _normalizer() -> TheTradeDeskNormalizer:
    return TheTradeDeskNormalizer(replace_empty_strings=True)


class TestNormalizer:
    """International-format values and % headers land on the schema columns."""

    def test_percent_headers(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Player 25% Complete") == "player_25pct_complete"
        assert normalizer.column_name("TTD Cost (USD)") == "ttd_cost_usd"
        assert normalizer.column_name("Ad Group Base Bid CPM (Adv Currency)") == ("ad_group_base_bid_cpm_adv_currency")

    def test_decimal_commas_and_day_first_dates(self):
        df = _normalizer().normalize([_ROW])
        record = df.to_dict("records")[0]
        assert record["date"] == dt.date(2026, 7, 10)  # day-first, not October 7
        assert record["ttd_cost_usd"] == 12.34
        # Strings containing commas that are not numbers pass through untouched.
        assert record["ad_group"] == "AG, One"

    def test_row_conforms_to_schema(self):
        df = _normalizer().normalize([_ROW])
        assert set(df.columns) <= set(schemas.AdGroupsStats.model_fields)
        table = TableData.from_data(df)
        conformed = table.reconcile(schemas.AdGroupsStats)
        TableData.from_data(conformed).reconcile(schemas.AdGroupsStats, strict=True)  # must not raise
        record = conformed.to_dict("records")[0]
        assert record["impressions"] == 1000
        assert record["player_25pct_complete"] == 7
        assert record["sampled_tracked_impressions"] is None  # "" -> None -> NA

    def test_the_report_keeps_the_international_normalizer_and_the_entities_flatten(self):
        normalizers = {type(asset).key: asset.normalizer for asset in _source().assets}
        assert isinstance(normalizers.pop("ad_groups_stats"), TheTradeDeskNormalizer)
        assert set(normalizers) == set(ENTITY_ASSETS)
        for key, normalizer in normalizers.items():
            assert not isinstance(normalizer, TheTradeDeskNormalizer), key
            assert normalizer.flatten_max_level == 1, key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_custom_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, TheTradeDeskNormalizer)
        assert normalizer.replace_empty_strings is True
        # International-format handling must survive in the child pod.
        record = normalizer.normalize([_ROW]).to_dict("records")[0]
        assert record["date"] == dt.date(2026, 7, 10)
        assert record["ttd_cost_usd"] == 12.34


def _page(path: tuple[str, ...], nodes: list[dict], cursor: str | None = None) -> dict:
    listing: dict = {"nodes": nodes, "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor}}
    for field in reversed(path):
        listing = {field: listing}
    return {"data": listing}


def _graphql(handler) -> tuple[TheTradeDeskConnection, list[dict]]:
    bodies: list[dict] = []

    def respond(request: httpx2.Request) -> httpx2.Response:
        assert str(request.url) == constants.GRAPHQL_URL
        body = json.loads(request.content)
        bodies.append(body)
        return httpx2.Response(200, json=handler(body))

    connection = TheTradeDeskConnection(id="ttd", api_key="x")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(respond))
    return connection, bodies


def _entity(connection: TheTradeDeskConnection, key: str) -> Any:
    source = TheTradeDesk(id="src-1", partner_id="qz94j5z", connection=connection)
    return next(a for a in source.assets if type(a).key == key)


def _snapshot(connection: TheTradeDeskConnection, key: str) -> list[dict]:
    asset = _entity(connection, key)
    context = il.ExecutionContext(asset, il.TimePartition(value=DAY).window)
    return asyncio.run(asset.data(context=context))


_CAMPAIGN = {
    "id": "c1",
    "name": "Campaign",
    "advertiser": {"id": "adv1"},
    "status": "Active",
    "createdAtUtc": "2026-09-01T08:00:00Z",
    "startDate": "2026-09-30T22:00:00.000Z",
    "endDate": None,
    "timeZone": "Europe/Berlin",
    "budget": {"total": 1000.5, "totalInImpressions": None},
    "primaryGoal": {"type": "CPA", "target": 2},
}


class TestGraphQL:
    """Entities come from The Trade Desk's GraphQL API, page by page."""

    def test_nodes_follow_the_cursor_until_the_last_page(self):
        def handler(body: dict) -> dict:
            if body["variables"]["after"] is None:
                return _page(("partner", "advertisers"), [{"id": "adv1"}], cursor="next")
            return _page(("partner", "advertisers"), [{"id": "adv2"}])

        connection, bodies = _graphql(handler)
        rows = asyncio.run(
            connection.nodes(constants.ADVERTISER_IDS_QUERY, {"partnerId": "p"}, ("partner", "advertisers"))
        )
        assert rows == [{"id": "adv1"}, {"id": "adv2"}]
        assert [body["variables"]["after"] for body in bodies] == [None, "next"]
        assert bodies[0]["variables"] == {"partnerId": "p", "first": constants.GRAPHQL_PAGE_SIZE, "after": None}

    def test_errors_answered_at_200_fail_the_query(self):
        connection, _ = _graphql(lambda body: {"errors": [{"message": "AUTHENTICATION_FAILURE"}], "data": None})
        with pytest.raises(RuntimeError, match="AUTHENTICATION_FAILURE"):
            asyncio.run(connection.nodes(constants.ADVERTISERS_QUERY, {"partnerId": "p"}, ("partner", "advertisers")))

    def test_an_unseen_parent_fails_rather_than_reading_as_no_nodes(self):
        connection, _ = _graphql(lambda body: {"data": {"advertiser": None}})
        with pytest.raises(RuntimeError, match="returned no advertiser"):
            asyncio.run(connection.nodes(constants.CAMPAIGNS_QUERY, {"advertiserId": "a"}, ("advertiser", "campaigns")))

    def test_a_parent_without_the_connection_reads_as_no_nodes(self):
        connection, _ = _graphql(lambda body: {"data": {"advertiser": {"campaigns": None}}})
        rows = asyncio.run(
            connection.nodes(constants.CAMPAIGNS_QUERY, {"advertiserId": "a"}, ("advertiser", "campaigns"))
        )
        assert rows == []

    def test_advertisers_are_the_partners_stamped_with_the_snapshot_day(self):
        connection, bodies = _graphql(lambda body: _page(("partner", "advertisers"), [{"id": "adv1", "name": "A"}]))
        assert _snapshot(connection, "advertisers") == [{"id": "adv1", "name": "A", "date": DAY}]
        assert bodies[0]["variables"]["partnerId"] == "qz94j5z"

    @pytest.mark.parametrize(("key", "field"), [("campaigns", "campaigns"), ("ad_groups", "adGroups")])
    def test_campaigns_and_ad_groups_are_walked_per_advertiser(self, key: str, field: str):
        def handler(body: dict) -> dict:
            if "partnerId" in body["variables"]:
                return _page(("partner", "advertisers"), [{"id": "adv1"}, {"id": "adv2"}])
            return _page(("advertiser", field), [{"id": f"x-{body['variables']['advertiserId']}"}])

        connection, bodies = _graphql(handler)
        rows = _snapshot(connection, key)
        assert sorted(row["id"] for row in rows) == ["x-adv1", "x-adv2"]
        assert {row["date"] for row in rows} == {DAY}
        assert sorted(body["variables"].get("advertiserId", "") for body in bodies[1:]) == ["adv1", "adv2"]

    def test_a_campaign_conforms_with_its_parent_id_flattened_and_its_schedule_as_utc_instants(self):
        def handler(body: dict) -> dict:
            if "partnerId" in body["variables"]:
                return _page(("partner", "advertisers"), [{"id": "adv1"}])
            return _page(("advertiser", "campaigns"), [_CAMPAIGN])

        connection, _ = _graphql(handler)
        frame = _entity(connection, "campaigns").normalizer.normalize(_snapshot(connection, "campaigns"))
        assert set(frame.columns) <= set(schemas.Campaigns.model_fields)
        record = TableData.from_data(frame).reconcile(schemas.Campaigns).to_dict("records")[0]
        assert (record["advertiser_id"], record["budget_total"], record["primary_goal_type"]) == ("adv1", 1000.5, "CPA")
        assert record["start_date"] == dt.datetime(2026, 9, 30, 22, tzinfo=dt.UTC)
        assert record["created_at_utc"] == dt.datetime(2026, 9, 1, 8, tzinfo=dt.UTC)
        assert record["time_zone"] == "Europe/Berlin"

    def test_entities_are_snapshots(self):
        for asset in _source().assets:
            if type(asset).key in ENTITY_ASSETS:
                assert isinstance(asset.partitioning, il.SnapshotPartitionConfig), type(asset).key
                assert asset.partitioning.column == "date"
                assert asset.tags == ["Entity"]
