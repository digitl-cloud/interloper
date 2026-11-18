"""Regression tests for the TheTradeDesk source.

MyReports are requested in "International" format: metric values carry decimal
commas ("12,34"), the date column is DD/MM/YYYY, and quartile headers spell
``%`` ("Player 25% Complete" → ``player_25pct_complete``). The custom
normalizer must convert all three so the generic casts succeed — and survive
the host→child DAG-spec round-trip. Each asset's requests and returned rows
are pinned against a mock transport.
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
from interloper_assets.thetradedesk.connector.normalizer import TheTradeDeskNormalizer
from interloper_assets.thetradedesk.connector.source import TheTradeDesk

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


class TestSourceNormalizer:
    """The report gets the International-format normalizer, the entities one flattening a level."""

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


def _run(connection: TheTradeDeskConnection, key: str) -> list[dict]:
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

    def test_a_campaign_conforms_with_its_parent_id_flattened_and_its_schedule_as_utc_instants(self):
        def handler(body: dict) -> dict:
            if "partnerId" in body["variables"]:
                return _page(("partner", "advertisers"), [{"id": "adv1"}])
            return _page(("advertiser", "campaigns"), [_CAMPAIGN])

        connection, _ = _graphql(handler)
        frame = _entity(connection, "campaigns").normalizer.normalize(_run(connection, "campaigns"))
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


def _recorded(handler) -> tuple[TheTradeDeskConnection, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def respond(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return handler(request)

    connection = TheTradeDeskConnection(id="ttd", api_key="x")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(respond))
    return connection, requests


_DOWNLOAD_URL = "https://download.example/delivery/report.csv"
_DELIVERED_URL = "https://download.example/files/report.csv"
_CSV = 'Date,Partner ID,TTD Cost (USD)\n01/10/2026,qz94j5z,"12,34"\n'


def _myreports(request: httpx2.Request) -> httpx2.Response:
    url = str(request.url)
    if url == f"{constants.BASE_URL}/myreports/reportschedule" and request.method == "POST":
        return httpx2.Response(200, json={"ReportScheduleId": 42})
    if url == f"{constants.BASE_URL}/myreports/reportexecution/query/partners":
        delivery = {"DownloadURL": _DOWNLOAD_URL}
        return httpx2.Response(
            200, json={"Result": [{"ReportExecutionState": "Complete", "ReportDeliveries": [delivery]}]}
        )
    if url == _DOWNLOAD_URL:
        return httpx2.Response(302, headers={"Location": _DELIVERED_URL})
    if url == _DELIVERED_URL:
        return httpx2.Response(200, text=_CSV)
    if url == f"{constants.BASE_URL}/myreports/reportschedule/42" and request.method == "DELETE":
        return httpx2.Response(200)
    raise AssertionError(f"unexpected request {request.method} {url}")


class TestRequests:
    """What each asset sends The Trade Desk, and the rows it returns."""

    def test_the_report_runs_a_one_time_schedule_for_its_day_and_deletes_it(self):
        connection, requests = _recorded(_myreports)

        rows = _run(connection, "ad_groups_stats")

        assert rows == [{"Date": "01/10/2026", "Partner ID": "qz94j5z", "TTD Cost (USD)": "12,34"}]
        assert [(request.method, str(request.url)) for request in requests] == [
            ("POST", f"{constants.BASE_URL}/myreports/reportschedule"),
            ("POST", f"{constants.BASE_URL}/myreports/reportexecution/query/partners"),
            ("GET", _DOWNLOAD_URL),
            ("GET", _DELIVERED_URL),
            ("DELETE", f"{constants.BASE_URL}/myreports/reportschedule/42"),
        ]
        assert json.loads(requests[0].content) == {
            "RequestedByUserName": "interloper",
            "IncludeHeaders": True,
            "PartnerFilters": ["qz94j5z"],
            "ReportDateFormat": "International",
            "ReportDateRange": "Custom",
            "ReportStartDateInclusive": "2026-10-01",
            "ReportEndDateExclusive": "2026-10-02",
            "ReportFileFormat": "CSV",
            "ReportFrequency": "Once",
            "ReportNumericFormat": "International",
            "ReportScheduleName": "interloper_168005_2026-10-01",
            "ReportTemplateId": "168005",
            "TimeZone": "UTC",
        }
        assert json.loads(requests[1].content) == {
            "PageStartIndex": 0,
            "PageSize": 100,
            "PartnerIds": ["qz94j5z"],
            "ReportScheduleIds": ["42"],
        }

    def test_advertisers_read_the_partners_advertisers_in_one_query(self):
        connection, bodies = _graphql(lambda body: _page(("partner", "advertisers"), [{"id": "adv1"}]))

        assert _run(connection, "advertisers") == [{"id": "adv1", "date": DAY}]
        assert bodies == [
            {
                "query": constants.ADVERTISERS_QUERY,
                "variables": {"partnerId": "qz94j5z", "first": constants.GRAPHQL_PAGE_SIZE, "after": None},
            }
        ]

    @pytest.mark.parametrize(
        ("key", "field", "query"),
        [("campaigns", "campaigns", constants.CAMPAIGNS_QUERY), ("ad_groups", "adGroups", constants.AD_GROUPS_QUERY)],
    )
    def test_advertiser_entities_list_the_advertisers_then_query_each(self, key: str, field: str, query: str):
        def handler(body: dict) -> dict:
            if "partnerId" in body["variables"]:
                return _page(("partner", "advertisers"), [{"id": "adv1"}, {"id": "adv2"}])
            return _page(("advertiser", field), [{"id": f"x-{body['variables']['advertiserId']}"}])

        connection, bodies = _graphql(handler)

        rows = _run(connection, key)

        assert sorted(rows, key=lambda row: row["id"]) == [{"id": "x-adv1", "date": DAY}, {"id": "x-adv2", "date": DAY}]
        assert bodies[0] == {
            "query": constants.ADVERTISER_IDS_QUERY,
            "variables": {"partnerId": "qz94j5z", "first": constants.GRAPHQL_PAGE_SIZE, "after": None},
        }
        assert sorted(bodies[1:], key=lambda body: body["variables"]["advertiserId"]) == [
            {
                "query": query,
                "variables": {"advertiserId": advertiser, "first": constants.GRAPHQL_PAGE_SIZE, "after": None},
            }
            for advertiser in ("adv1", "adv2")
        ]
