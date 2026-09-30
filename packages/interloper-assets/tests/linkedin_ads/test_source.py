"""Regression tests for the LinkedIn Ads source.

Ad analytics rows carry their day as a nested ``dateRange`` of ``{year, month,
day}`` objects; ``LinkedinAdsStatsNormalizer`` turns both bounds into dates so
the report partitions on the vendor's own ``date_range_start``. Entities come
back undated and carry epoch-millisecond ``runSchedule`` instants, which the
source's ``LinkedinAdsNormalizer`` converts to UTC datetimes. Requests are
written as raw Rest.li 2.0 query strings, pinned here against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import httpx2
import interloper as il
from interloper.asset.context import ExecutionContext
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation

from interloper_assets.linkedin_ads import constants, schemas
from interloper_assets.linkedin_ads.connection import LinkedinAdsConnection
from interloper_assets.linkedin_ads.source import LinkedinAds, LinkedinAdsNormalizer, LinkedinAdsStatsNormalizer

REPORT_ASSETS = ("ads_stats",)
ENTITY_ASSETS = ("ad_accounts", "campaign_groups", "campaigns")
DAY = dt.date(2026, 9, 15)


def _source(connection: LinkedinAdsConnection | None = None) -> Any:
    if connection is None:
        return LinkedinAds(id="src-1", account_id="123")
    return LinkedinAds(id="src-1", account_id="123", connection=connection)


def _asset(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


def _faked_source(handler: Any) -> Any:
    connection = LinkedinAdsConnection(client_id="cid", client_secret="secret", refresh_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        "https://api.linkedin.com/rest", transport=httpx2.MockTransport(handler)
    )
    return _source(connection)


async def _run(source: Any, key: str) -> list[dict[str, Any]]:
    asset = _asset(source, key)
    context = ExecutionContext(
        asset_key=asset.key,
        partitioning=asset.partitioning,
        partition_or_window=il.TimePartition(value=DAY),
    )
    return await asset.data(context=context)


ANALYTICS_ROW = {
    "pivotValues": ["urn:li:share:7001", "urn:li:sponsoredCampaign:42"],
    "dateRange": {"start": {"year": 2026, "month": 9, "day": 15}, "end": {"year": 2026, "month": 9, "day": 15}},
    "costInLocalCurrency": "19.91833",
    "impressions": 165,
    "clicks": 11,
    "externalWebsiteConversions": 0,
    "averageDwellTime": 2.4,
    "audiencePenetration": 0.013,
    "approximateMemberReach": 120,
}

CAMPAIGN_ROW = {
    "id": 42,
    "name": "Autumn launch",
    "type": "SPONSORED_UPDATES",
    "status": "ACTIVE",
    "objectiveType": "WEBSITE_VISIT",
    "dailyBudget": {"amount": "18", "currencyCode": "EUR"},
    "runSchedule": {"start": 1757894400000, "end": 1760486400000},
    "costType": "CPM",
    "campaignGroup": "urn:li:sponsoredCampaignGroup:7",
}


class TestSourceNormalizer:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == set(REPORT_ASSETS + ENTITY_ASSETS)

    def test_report_uses_the_stats_normalizer(self):
        assert isinstance(_asset(_source(), "ads_stats").normalizer, LinkedinAdsStatsNormalizer)

    def test_entities_use_the_source_normalizer(self):
        for key in ENTITY_ASSETS:
            normalizer = _asset(_source(), key).normalizer
            assert isinstance(normalizer, LinkedinAdsNormalizer), key
            assert normalizer.flatten_max_level == 1, key
            assert normalizer.epoch_columns == ["run_schedule_start", "run_schedule_end"], key


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        source = _source()
        asset = _asset(source, key)
        spec_json = DAG(source).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_every_normalizer_survives_the_roundtrip(self):
        for key in REPORT_ASSETS + ENTITY_ASSETS:
            parent = _asset(_source(), key).normalizer
            child = self._child(key).normalizer
            assert type(child) is type(parent), key
            assert child.model_dump() == parent.model_dump(), key

    def test_analytics_row_reconciles_with_dates_and_typed_metrics(self):
        child = self._child("ads_stats")
        reconciled = Representation.of(child.normalizer.normalize([ANALYTICS_ROW])).reconcile(schemas.AdsStats)
        row = reconciled.iloc[0]
        assert row["date_range_start"] == DAY
        assert row["date_range_end"] == DAY
        assert row["pivot_values"] == '["urn:li:share:7001", "urn:li:sponsoredCampaign:42"]'
        assert float(row["cost_in_local_currency"]) == 19.91833
        assert int(row["impressions"]) == 165
        assert int(row["approximate_member_reach"]) == 120
        assert float(row["average_dwell_time"]) == 2.4
        assert "date" not in reconciled.columns

    def test_campaign_row_reconciles_with_flattened_budget_and_schedule(self):
        child = self._child("campaigns")
        normalized = child.normalizer.normalize([{**CAMPAIGN_ROW, "date": DAY}])
        row = Representation.of(normalized).reconcile(schemas.Campaigns).iloc[0]
        assert int(row["id"]) == 42
        assert float(row["daily_budget_amount"]) == 18.0
        assert row["daily_budget_currency_code"] == "EUR"
        assert row["run_schedule_start"] == dt.datetime(2025, 9, 15, tzinfo=dt.timezone.utc)
        assert row["campaign_group"] == "urn:li:sponsoredCampaignGroup:7"
        assert row["date"] == DAY

    def test_open_ended_schedule_leaves_the_end_empty(self):
        rows = [{**CAMPAIGN_ROW, "runSchedule": {"start": 1757894400000}, "date": DAY}]
        normalized = LinkedinAdsNormalizer(flatten_max_level=1, epoch_columns=["run_schedule_end"]).normalize(rows)
        assert "run_schedule_end" not in normalized.columns


class TestPartitionColumns:
    def test_report_partitions_on_the_vendor_day(self):
        asset = _asset(_source(), "ads_stats")
        assert asset.partitioning.column == "date_range_start"
        assert asset.schema.model_fields["date_range_start"].annotation == (dt.date | None)
        assert "date" not in asset.schema.model_fields

    def test_entities_partition_on_a_stamped_date(self):
        for key in ENTITY_ASSETS:
            asset = _asset(_source(), key)
            assert asset.partitioning.column == "date", key
            assert "date" in asset.schema.model_fields, key


class TestRequests:
    async def test_ads_stats_sends_a_raw_restli_query(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"elements": [ANALYTICS_ROW]})

        rows = await _run(_faked_source(handler), "ads_stats")
        assert rows == [ANALYTICS_ROW]
        query = seen[0].url.query.decode()
        assert "pivots=List(SHARE,CAMPAIGN)" in query
        assert "dateRange=(start:(year:2026,month:9,day:15),end:(year:2026,month:9,day:15))" in query
        assert "accounts=List(urn%3Ali%3AsponsoredAccount%3A123)" in query
        assert "fields=pivotValues,dateRange,costInLocalCurrency," in query

    async def test_ad_accounts_fetches_the_sources_account(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"id": 123, "name": "Acme", "currency": "EUR", "status": "ACTIVE"})

        rows = await _run(_faked_source(handler), "ad_accounts")
        assert seen[0].url.path == "/rest/adAccounts/123"
        assert rows == [{"id": 123, "name": "Acme", "currency": "EUR", "status": "ACTIVE", "date": DAY}]

    async def test_search_follows_the_page_token(self):
        seen: list[str] = []
        pages = {
            None: {"elements": [{"id": 1}], "metadata": {"nextPageToken": "tok/1"}},
            "tok%2F1": {"elements": [{"id": 2}], "metadata": {}},
        }

        def handler(request: httpx2.Request) -> httpx2.Response:
            query = request.url.query.decode()
            seen.append(query)
            token = query.split("pageToken=")[1] if "pageToken=" in query else None
            return httpx2.Response(200, json=pages[token])

        rows = await _run(_faked_source(handler), "campaigns")
        assert [row["id"] for row in rows] == [1, 2]
        assert all(row["date"] == DAY for row in rows)
        assert len(seen) == 2
        assert seen[0].startswith("q=search&pageSize=1000&fields=id,name,type,status,")
        assert seen[1].endswith("&pageToken=tok%2F1")

    async def test_campaign_groups_search_their_own_resource(self):
        seen: list[httpx2.URL] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request.url)
            return httpx2.Response(200, json={"elements": [{"id": 7}], "metadata": {}})

        rows = await _run(_faked_source(handler), "campaign_groups")
        assert rows == [{"id": 7, "date": DAY}]
        assert [url.path for url in seen] == ["/rest/adAccounts/123/adCampaignGroups"]
        assert seen[0].query.decode() == f"q=search&pageSize=1000&fields={','.join(constants.CAMPAIGN_GROUP_FIELDS)}"

    def test_epoch_unit_is_configurable(self):
        normalizer = LinkedinAdsNormalizer(flatten_max_level=1, epoch_columns=["run_schedule_start"], epoch_unit="s")
        normalized = normalizer.normalize([{"runSchedule": {"start": 1757894400}}])
        assert normalized.loc[0, "run_schedule_start"] == dt.datetime(2025, 9, 15, tzinfo=dt.timezone.utc)
