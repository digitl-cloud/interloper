"""Regression tests for the Criteo source configuration.

The Criteo statistics report returns PascalCase columns (``Day``, ``AdId``,
``SalesAllClientAttribution``, …) while the schemas are snake_case. A
source-level ``DataFrameNormalizer`` bridges the two — these tests pin that
the normalizer reaches every asset instance, survives the host→child spec
round-trip, and that a raw API-shaped row normalizes and validates against
the schema (the same chain that broke in prod for AmazonAds; see
``amazon_ads/test_source.py``). The entity assets are pinned against
responses shaped like Criteo's published 2026-07 spec: every field of a
resource reaches the schema.
"""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.criteo import constants
from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.connector.schemas import AdsStats, CampaignsStats
from interloper_assets.criteo.connector.source import Criteo

# Raw API column names per asset, as Criteo returns them: the requested id
# dimensions, their companion name/currency columns, and the full metric set.
ADS_COLUMNS = [
    "Day",
    "AdId",
    "Ad",
    "AdsetId",
    "Adset",
    "CampaignId",
    "Campaign",
    "Currency",
    *constants.STATISTICS_METRICS,
]
CAMPAIGNS_COLUMNS = ["Day", "CampaignId", "Campaign", "Currency", *constants.STATISTICS_METRICS]


def _source() -> Any:
    return Criteo(
        id="src-1",
        advertiser_id="123",
        connection=CriteoConnection(client_id="x", client_secret="x", refresh_token="x"),
    )


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer and conform."""

    def _child_asset(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_ads_row_conforms_after_roundtrip(self):
        child = self._child_asset("ads_stats")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        row: dict[str, object] = dict.fromkeys(ADS_COLUMNS)
        row["Day"] = "2026-06-10"
        normalized = child.normalizer.normalize(pd.DataFrame([row]))
        TableData.from_data(normalized).reconcile(AdsStats)  # must not raise

    def test_campaigns_row_conforms_after_roundtrip(self):
        child = self._child_asset("campaigns_stats")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        row: dict[str, object] = dict.fromkeys(CAMPAIGNS_COLUMNS)
        row["Day"] = "2026-06-10"
        normalized = child.normalizer.normalize(pd.DataFrame([row]))
        TableData.from_data(normalized).reconcile(CampaignsStats)  # must not raise


def _report_query(**fields: Any) -> dict[str, Any]:
    """Run the campaign report of a source configured with *fields* against a mock Criteo.

    Returns:
        The report request's JSON body.
    """
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json={"Rows": []})

    connection = CriteoConnection(id="criteo", client_id="cid", client_secret="x", refresh_token="x")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    source = Criteo(id="src-1", advertiser_id="123", connection=connection, **fields)
    asset = next(a for a in source.assets if type(a).key == "campaigns_stats")
    window = il.TimePartition(value=dt.date(2026, 10, 26)).window
    asyncio.run(asset.data(context=il.ExecutionContext(asset, window)))
    assert [request.url.path for request in requests] == ["/2026-07/statistics/report"]
    return json.loads(requests[0].content)


class TestReportCurrencyAndDays:
    """A report is requested in euros, since Criteo states no advertiser's currency, and in the source's days."""

    def test_the_report_is_in_euros_and_utc_days_by_default(self):
        query = _report_query()
        assert (query["currency"], query["timezone"]) == ("EUR", "UTC")

    def test_the_report_days_are_in_the_time_zone_the_source_sets(self):
        query = _report_query(timezone="Europe/Zurich")
        assert (query["currency"], query["timezone"]) == ("EUR", "Europe/Zurich")


class TestApiVersion:
    """Every call goes to the supported API version; 2025-04 is past Criteo's support window."""

    def test_the_reports_and_the_advertiser_list_call_version_2026_07(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.path.endswith("/advertisers/me"):
                return httpx2.Response(200, json={"data": [{"id": "123", "attributes": {"advertiserName": "Example"}}]})
            return httpx2.Response(200, json={"Rows": [{"Day": "2026-10-26", "CampaignId": "7"}]})

        connection = CriteoConnection(id="criteo", client_id="cid", client_secret="x", refresh_token="x")
        connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
        source = Criteo(id="src-1", advertiser_id="123", connection=connection)
        asset = next(a for a in source.assets if type(a).key == "campaigns_stats")
        partition = il.TimePartition(value=dt.date(2026, 10, 26))

        rows = asyncio.run(asset.data(context=il.ExecutionContext(asset, partition.window)))
        advertisers = asyncio.run(connection.advertisers())

        assert [request.url.path for request in requests] == ["/2026-07/statistics/report", "/2026-07/advertisers/me"]
        assert json.loads(requests[0].content)["dimensions"] == ["Day", "CampaignId"]
        assert rows == [{"Day": "2026-10-26", "CampaignId": "7"}]
        assert advertisers == [{"id": "123", "name": "Example"}]


SNAPSHOT_DAY = dt.date(2026, 11, 9)
ENTITY_ASSETS = ("advertisers", "campaigns", "ad_sets", "ads")
CAMPAIGN: dict[str, Any] = {
    "id": "7",
    "type": "Campaign",
    "attributes": {
        "id": "7",
        "advertiserId": "123",
        "name": "Prospecting",
        "goal": "acquisition",
        "spendLimit": {
            "spendLimitType": "capped",
            "spendLimitRenewal": "monthly",
            "spendLimitAmount": {"value": 900.5},
        },
        "budgetAutomation": {
            "enabled": True,
            "automatedBudgetConfiguration": {"adSetOptimizationObjective": "revenue"},
        },
    },
}
AD_SET: dict[str, Any] = {
    "id": "70",
    "type": "AdSet",
    "attributes": {
        "name": "Retargeting EU",
        "advertiserId": "123",
        "campaignId": "7",
        "datasetId": "55",
        "destinationEnvironment": "web",
        "mediaType": "display",
        "objective": "conversions",
        "videoChannel": "undefined",
        "schedule": {
            "startDate": {"value": "2026-10-01T00:00:00Z"},
            "endDate": {"value": "2026-12-31T00:00:00Z"},
            "activationStatus": "on",
            "deliveryStatus": "live",
        },
        "bidding": {"bidAmount": 0.35, "costController": "maxCPC"},
        "budget": {
            "budgetStrategy": "capped",
            "budgetRenewal": "daily",
            "budgetDeliverySmoothing": "standard",
            "budgetDeliveryWeek": "undefined",
            "budgetAmount": 40.0,
            "pacingBehavior": "targetFullBudget",
        },
        "attributionConfiguration": {"attributionMethod": "lastClick", "lookbackWindow": "30D"},
        "targeting": {
            "deliveryLimitations": {"environments": ["web"], "devices": [], "operatingSystems": ["ios"]},
            "geoLocation": {
                "countries": {"value": {"operand": "in", "values": ["DE", "AT"]}},
                "subdivisions": {"value": {"operand": "notIn", "values": ["DE-BY"]}},
                "zipCodes": {"value": {"operand": "in", "values": ["10115"]}},
            },
            "frequencyCapping": {"value": {"frequency": "daily", "maximumImpressions": 6}},
        },
    },
}
AD: dict[str, Any] = {
    "id": "700",
    "type": "Ad",
    "attributes": {
        "id": "700",
        "name": "Banner",
        "description": None,
        "creativeId": "9",
        "adSetId": "70",
        "inventoryType": "Display",
        "startDate": "2026-10-01",
        "endDate": None,
    },
}


def _connected(handler: Any) -> Any:
    connection = CriteoConnection(id="criteo", client_id="cid", client_secret="x", refresh_token="x")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    return Criteo(id="src-1", advertiser_id="123", connection=connection)


def _entity(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


async def _snapshot(source: Any, key: str) -> list[dict[str, Any]]:
    """Run an entity asset, then normalize and reconcile its rows as the runner would.

    Returns:
        The reconciled rows, NULLs as ``None``.
    """
    asset = _entity(source, key)
    rows = await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=SNAPSHOT_DAY).window))
    reconciled = TableData.from_data(asset.normalizer.normalize(rows)).reconcile(asset.schema)
    return reconciled.astype(object).where(reconciled.notna(), None).to_dict("records")


class TestEntityAssets:
    """Criteo's entities are snapshots of its JSON:API resources, every field kept."""

    def test_entities_are_snapshots_tagged_entity(self):
        for key in ENTITY_ASSETS:
            asset = _entity(_source(), key)
            assert isinstance(asset.partitioning, il.SnapshotPartitionConfig), key
            assert asset.partitioning.column == "date", key
            assert asset.tags == ["Entity"], key

    async def test_an_advertiser_the_connection_no_longer_sees_fails(self):
        other = {"id": "999", "type": "Advertiser", "attributes": {"advertiserName": "Other"}}
        source = _connected(lambda request: httpx2.Response(200, json={"data": [other], "errors": [], "warnings": []}))
        with pytest.raises(RuntimeError, match="does not list advertiser 123"):
            await _snapshot(source, "advertisers")

    async def test_the_advertiser_is_the_sources_own(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.url.path == "/2026-07/advertisers/me"
            resources = [
                {"id": "999", "type": "Advertiser", "attributes": {"advertiserName": "Other"}},
                {"id": "123", "type": "Advertiser", "attributes": {"advertiserName": "Example"}, "meta": {"k": 1}},
            ]
            return httpx2.Response(200, json={"data": resources, "errors": [], "warnings": []})

        [row] = await _snapshot(_connected(handler), "advertisers")
        assert row == {
            "date": SNAPSHOT_DAY,
            "id": "123",
            "type": "Advertiser",
            "meta": '{"k": 1}',
            "advertiser_name": "Example",
        }

    async def test_campaigns_are_searched_by_advertiser_and_flattened(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [CAMPAIGN], "errors": [], "warnings": []})

        [row] = await _snapshot(_connected(handler), "campaigns")
        assert [request.url.path for request in requests] == ["/2026-07/marketing-solutions/campaigns/search"]
        assert json.loads(requests[0].content) == {"filters": {"advertiserIds": ["123"]}}
        assert row == {
            "date": SNAPSHOT_DAY,
            "id": "7",
            "type": "Campaign",
            "advertiser_id": "123",
            "name": "Prospecting",
            "goal": "acquisition",
            "spend_limit_spend_limit_type": "capped",
            "spend_limit_spend_limit_renewal": "monthly",
            "spend_limit_spend_limit_amount_value": 900.5,
            "budget_automation_enabled": True,
            "budget_automation_automated_budget_configuration_ad_set_optimization_objective": "revenue",
        }

    async def test_ad_sets_are_searched_by_advertiser_and_flattened(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [AD_SET], "errors": [], "warnings": []})

        [row] = await _snapshot(_connected(handler), "ad_sets")
        assert [request.url.path for request in requests] == ["/2026-07/marketing-solutions/ad-sets/search"]
        assert json.loads(requests[0].content) == {"filters": {"advertiserIds": ["123"]}}
        assert (row["id"], row["type"], row["campaign_id"], row["name"]) == ("70", "AdSet", "7", "Retargeting EU")
        assert row["schedule_start_date_value"] == pd.Timestamp("2026-10-01T00:00:00Z")
        assert (row["schedule_activation_status"], row["schedule_delivery_status"]) == ("on", "live")
        assert (row["budget_budget_amount"], row["budget_budget_renewal"]) == (40.0, "daily")
        assert row["targeting_geo_location_countries_value_values"] == '["DE", "AT"]'
        assert row["targeting_frequency_capping_value_maximum_impressions"] == 6
        assert row["targeting_delivery_limitations_devices"] == "[]"

    async def test_every_field_of_an_ad_set_is_declared(self):
        source = _connected(lambda request: httpx2.Response(200, json={"data": [AD_SET], "errors": [], "warnings": []}))
        asset = _entity(source, "ad_sets")
        rows = await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=SNAPSHOT_DAY).window))
        assert set(asset.normalizer.normalize(rows).columns) == set(asset.schema.model_fields)

    async def test_an_unset_schedule_end_is_empty(self):
        resource = copy.deepcopy(AD_SET)
        resource["attributes"]["schedule"]["endDate"] = {"value": None}

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(200, json={"data": [resource], "errors": [], "warnings": []})

        [row] = await _snapshot(_connected(handler), "ad_sets")
        assert row["schedule_end_date_value"] is None

    async def test_ads_are_paged_until_an_empty_page(self):
        offsets: list[str | None] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.url.path == "/2026-07/marketing-solutions/advertisers/123/ads"
            offsets.append(request.url.params.get("offset"))
            data = [AD] if request.url.params.get("offset") == "0" else []
            return httpx2.Response(200, json={"data": data, "errors": [], "warnings": []})

        [row] = await _snapshot(_connected(handler), "ads")
        assert offsets == ["0", "50"]
        assert row == {
            "date": SNAPSHOT_DAY,
            "id": "700",
            "type": "Ad",
            "name": "Banner",
            "description": None,
            "creative_id": "9",
            "ad_set_id": "70",
            "inventory_type": "Display",
            "start_date": dt.date(2026, 10, 1),
            "end_date": None,
        }

    async def test_a_refused_scope_fails_the_asset(self):
        source = _connected(lambda request: httpx2.Response(403, json={"errors": [{"title": "Forbidden"}]}))
        asset = _entity(source, "campaigns")
        with pytest.raises(httpx2.HTTPStatusError):
            await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=SNAPSHOT_DAY).window))


REPORT_DAY = dt.date(2026, 10, 26)


async def _sent(
    key: str, respond: Callable[[httpx2.Request], dict[str, Any]], day: dt.date = SNAPSHOT_DAY
) -> tuple[list[dict[str, Any]], list[httpx2.Request]]:
    """Run one asset of a source against a mock Criteo that answers each request with *respond*'s body.

    Args:
        key: The asset to run.
        respond: The JSON body of Criteo's answer to each request, given the request.
        day: The partition the asset runs for.

    Returns:
        The asset's rows as it returns them, and every request it sent, in order.
    """
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json=respond(request))

    asset = _entity(_connected(handler), key)
    rows = await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=day).window))
    return rows, requests


def _entities(*resources: dict[str, Any]) -> Callable[[httpx2.Request], dict[str, Any]]:
    return lambda request: {"data": list(resources), "errors": [], "warnings": []}


class TestRequests:
    """What each asset sends to Criteo, request by request, and the rows it returns."""

    @pytest.mark.parametrize(
        ("key", "dimensions"),
        [("ads_stats", ["Day", "AdId", "AdsetId", "CampaignId"]), ("campaigns_stats", ["Day", "CampaignId"])],
    )
    async def test_a_report_posts_its_query_for_its_day(self, key: str, dimensions: list[str]):
        report_row = {"Day": "2026-10-26", "CampaignId": "7", "Currency": "EUR", "Clicks": 3}

        rows, requests = await _sent(key, lambda request: {"Rows": [report_row], "Total": {}}, day=REPORT_DAY)

        (request,) = requests
        assert (request.method, request.url.path) == ("POST", "/2026-07/statistics/report")
        assert not request.url.params
        assert json.loads(request.content) == {
            "advertiserIds": "123",
            "dimensions": dimensions,
            "startDate": "2026-10-26",
            "endDate": "2026-10-26",
            "currency": "EUR",
            "timezone": "UTC",
            "metrics": constants.STATISTICS_METRICS,
            "format": "json",
        }
        assert rows == [report_row]

    async def test_a_report_without_rows_is_empty(self):
        rows, _ = await _sent("campaigns_stats", lambda request: {"Rows": [], "Total": {}}, day=REPORT_DAY)

        assert rows == []

    async def test_the_advertiser_is_read_from_the_connections_advertisers(self):
        advertiser = {"id": "123", "type": "Advertiser", "attributes": {"advertiserName": "Example"}}

        rows, requests = await _sent("advertisers", _entities(advertiser))

        (request,) = requests
        assert (request.method, request.url.path) == ("GET", "/2026-07/advertisers/me")
        assert not request.url.params
        assert not request.content
        assert rows == [{"advertiserName": "Example", "id": "123", "type": "Advertiser", "date": SNAPSHOT_DAY}]

    @pytest.mark.parametrize(
        ("key", "path", "resource"),
        [
            ("campaigns", "/2026-07/marketing-solutions/campaigns/search", CAMPAIGN),
            ("ad_sets", "/2026-07/marketing-solutions/ad-sets/search", AD_SET),
        ],
    )
    async def test_a_search_posts_the_advertisers_filter(self, key: str, path: str, resource: dict[str, Any]):
        rows, requests = await _sent(key, _entities(resource))

        (request,) = requests
        assert (request.method, request.url.path) == ("POST", path)
        assert not request.url.params
        assert json.loads(request.content) == {"filters": {"advertiserIds": ["123"]}}
        fields = {"id": resource["id"], "type": resource["type"], "date": SNAPSHOT_DAY}
        assert rows == [{**resource["attributes"], **fields}]

    async def test_a_search_without_data_is_empty(self):
        rows, _ = await _sent("campaigns", lambda request: {"data": None, "errors": [], "warnings": []})

        assert rows == []

    async def test_ads_are_read_fifty_at_a_time(self):
        second = {**AD, "id": "701", "attributes": {**AD["attributes"], "id": "701"}}
        queries: list[dict[str, str]] = []

        def respond(request: httpx2.Request) -> dict[str, Any]:
            queries.append(dict(request.url.params))
            pages = {"0": [AD], "50": [second]}
            return {"data": pages.get(request.url.params["offset"], []), "errors": [], "warnings": []}

        rows, requests = await _sent("ads", respond)

        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/2026-07/marketing-solutions/advertisers/123/ads")
        ] * 3
        assert queries == [
            {"limit": "50", "offset": "0"},
            {"limit": "50", "offset": "50"},
            {"limit": "50", "offset": "100"},
        ]
        assert rows == [
            {**AD["attributes"], "id": "700", "type": "Ad", "date": SNAPSHOT_DAY},
            {**second["attributes"], "id": "701", "type": "Ad", "date": SNAPSHOT_DAY},
        ]

    async def test_a_refused_report_raises_the_http_error(self):
        source = _connected(lambda request: httpx2.Response(401, json={"errors": [{"title": "Unauthorized"}]}))
        asset = _entity(source, "ads_stats")

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=REPORT_DAY).window))
