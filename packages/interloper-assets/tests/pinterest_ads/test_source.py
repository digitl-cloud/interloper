"""Regression tests for the PinterestAds source.

Reports come from Pinterest's async analytics flow (create, poll, download a
signed JSON file mapping each entity id to its rows), requested at ``DAY``
granularity so every row carries its ``DATE``; their UPPER_SNAKE columns
snake-case onto the schemas, with ``column_overrides`` for the ``3SEC`` /
``15SEC`` names. Entities are bookmark-paginated objects with nested settings
and Unix-second timestamps, handled by ``PinterestEntityNormalizer``. These
tests pin the key set, both reshapes, the spec round-trip, the partition
columns, and the report and pagination helpers against a faked API.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.pinterest_ads import constants, schemas
from interloper_assets.pinterest_ads import source as source_module
from interloper_assets.pinterest_ads.connection import PinterestAdsConnection
from interloper_assets.pinterest_ads.source import PinterestAds, PinterestEntityNormalizer

REPORT_ASSETS = ("ads_stats", "campaigns_stats", "ads_conversions_stats", "videos_stats_by_targeting")
ENTITY_ASSETS = ("ad_accounts", "campaigns", "ad_groups", "ads")
DAY = dt.date(2026, 9, 14)


def _source() -> Any:
    return PinterestAds(id="src-1", account_id="549759962542")


def _asset(key: str) -> Any:
    return next(a for a in _source().assets if type(a).key == key)


def _child(key: str) -> Any:
    src = _source()
    asset = next(a for a in src.assets if type(a).key == key)
    spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
    child_dag = DAGSpec(**spec_json).reconstruct()
    return next(a for a in child_dag.operations if type(a).key == key)


async def _run(source: Any, key: str) -> list[dict[str, Any]]:
    asset = next(a for a in source.assets if type(a).key == key)
    context = il.ExecutionContext(
        asset_key=asset.key,
        partitioning=asset.partitioning,
        partition_or_window=il.TimePartition(value=DAY),
    )
    return await asset.data(context=context)


def _source_with_api(handler: Any) -> Any:
    connection = PinterestAdsConnection(client_id="cid", client_secret="secret", refresh_token="refresh")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    return PinterestAds(id="src-1", account_id="549759962542", connection=connection)


class TestAssets:
    def test_all_eight_assets_present(self):
        assert {type(a).key for a in _source().assets} == {*REPORT_ASSETS, *ENTITY_ASSETS}

    def test_reports_use_the_source_normalizer(self):
        for key in REPORT_ASSETS:
            normalizer = _asset(key).normalizer
            assert type(normalizer) is DataFrameNormalizer, key
            assert normalizer.column_overrides["VIDEO_3SEC_VIEWS_1"] == "video_3sec_views_1", key

    def test_entities_use_the_entity_normalizer(self):
        for key in ENTITY_ASSETS:
            normalizer = _asset(key).normalizer
            assert isinstance(normalizer, PinterestEntityNormalizer), key
            assert normalizer.flatten_max_level == 1, key
            assert normalizer.epoch_columns == ["created_time", "updated_time", "start_time", "end_time"], key
            assert normalizer.epoch_unit == "s", key

    def test_campaigns_stats_requests_no_ad_level_columns(self):
        declared = set(_asset("campaigns_stats").schema.model_fields)
        for column in ("ad_group_id", "ad_name", "pin_id", "pin_promotion_status", "product_group_id"):
            assert column.upper() not in constants.CAMPAIGN_METRICS and column not in declared, column

    def test_every_requested_column_is_declared(self):
        requested = {
            "ads_stats": [*constants.AD_METRICS, "AD_ID"],
            "campaigns_stats": constants.CAMPAIGN_METRICS,
            "ads_conversions_stats": constants.ADS_CONVERSIONS_METRICS,
            "videos_stats_by_targeting": constants.VIDEOS_METRICS,
        }
        for key, columns in requested.items():
            normalizer = _asset(key).normalizer
            declared = set(_asset(key).schema.model_fields)
            missing = [c for c in columns if normalizer.column_name(c) not in declared]
            assert missing == [], key


class TestPartitionColumns:
    def test_reports_partition_on_the_vendor_date(self):
        for key in REPORT_ASSETS:
            asset = _asset(key)
            assert asset.partitioning.column == "date", key
            assert asset.schema.model_fields["date"].description == "The report day.", key
            assert asset.tags == ["Report"], key

    def test_entities_partition_on_the_stamped_date(self):
        for key in ENTITY_ASSETS:
            asset = _asset(key)
            assert asset.partitioning.column == "date", key
            assert asset.tags == ["Entity"], key


class TestReconcile:
    """Vendor-shaped rows survive the spec round-trip and reconcile with the right types."""

    def test_ads_stats_row(self):
        child = _child("ads_stats")
        rows = [
            {
                "DATE": "2026-09-14",
                "AD_ACCOUNT_ID": "549759962542",
                "AD_ID": 687000000001,
                "CAMPAIGN_ID": 626000000001,
                "CPC_IN_MICRO_DOLLAR": 412345.67,
                "SPEND_IN_MICRO_DOLLAR": 12500000,
                "CTR": 0.0123,
                "PAID_IMPRESSION": 1000.0,
                "TOTAL_CLICKTHROUGH": 12,
            }
        ]
        reconciled = Representation.of(child.normalizer.normalize(rows)).reconcile(schemas.AdsStats)
        row = reconciled.iloc[0]
        assert row["date"] == DAY
        assert row["ad_id"] == "687000000001"
        assert row["campaign_id"] == "626000000001"
        assert float(row["cpc_in_micro_dollar"]) == pytest.approx(412345.67)
        assert float(row["spend_in_micro_dollar"]) == 12500000.0
        assert int(row["paid_impression"]) == 1000
        assert int(row["total_clickthrough"]) == 12

    def test_video_row_keeps_the_sec_names_and_targeting(self):
        child = _child("videos_stats_by_targeting")
        rows = [
            {
                "DATE": "2026-09-14",
                "PIN_PROMOTION_ID": 687000000001,
                "TARGETING_TYPE": "APPTYPE",
                "TARGETING_VALUE": "iphone",
                "VIDEO_3SEC_VIEWS_1": 40,
                "TOTAL_VIDEO_15SEC_UNIQUE_VIEWS": 7,
                "TOTAL_VIDEO_AVG_WATCHTIME_IN_SECOND": 3.2,
                "VIDEO_LENGTH": 15.0,
            }
        ]
        normalized = child.normalizer.normalize(rows)
        assert "video_3sec_views_1" in normalized.columns
        assert "total_video_15sec_unique_views" in normalized.columns
        reconciled = Representation.of(normalized).reconcile(schemas.VideosStatsByTargeting)
        row = reconciled.iloc[0]
        assert row["targeting_type"] == "APPTYPE"
        assert row["targeting_value"] == "iphone"
        assert int(row["video_3sec_views_1"]) == 40
        assert float(row["total_video_avg_watchtime_in_second"]) == pytest.approx(3.2)

    def test_campaign_row_converts_unix_seconds(self):
        child = _child("campaigns")
        assert child.normalizer.epoch_columns == _asset("campaigns").normalizer.epoch_columns
        assert child.normalizer.epoch_unit == "s"
        rows = [
            {
                "id": "626000000001",
                "ad_account_id": "549759962542",
                "created_time": 1757808000,
                "end_time": None,
                "daily_spend_cap": 25000000,
                "is_campaign_budget_optimization": False,
                "tracking_urls": {"click": ["https://t.example/c"], "impression": []},
                "bid_options": {"gender_multipliers": {"female": 1.2}},
                "date": DAY,
            }
        ]
        normalized = child.normalizer.normalize(rows)
        reconciled = Representation.of(normalized).reconcile(schemas.Campaigns)
        row = reconciled.iloc[0]
        assert row["created_time"] == dt.datetime(2025, 9, 14, tzinfo=dt.timezone.utc)
        assert float(row["daily_spend_cap"]) == 25000000.0
        assert json.loads(row["tracking_urls_click"]) == ["https://t.example/c"]
        assert json.loads(row["bid_options_gender_multipliers"]) == {"female": 1.2}
        assert row["date"] == DAY

    def test_ad_group_targeting_spec_flattens(self):
        child = _child("ad_groups")
        rows = [
            {
                "id": "2680000000001",
                "targeting_spec": {"APPTYPE": ["iphone", "web"], "MINIMUM_AGE": "25"},
                "start_time": 1757808000,
                "date": DAY,
            }
        ]
        reconciled = Representation.of(child.normalizer.normalize(rows)).reconcile(schemas.AdGroups)
        row = reconciled.iloc[0]
        assert json.loads(row["targeting_spec_apptype"]) == ["iphone", "web"]
        assert row["targeting_spec_minimum_age"] == "25"
        assert row["start_time"] == dt.datetime(2025, 9, 14, tzinfo=dt.timezone.utc)

    def test_ad_account_owner_flattens(self):
        child = _child("ad_accounts")
        rows = [
            {"id": "549759962542", "owner": {"id": "9", "username": "brand"}, "permissions": ["ADMIN"], "date": DAY}
        ]
        reconciled = Representation.of(child.normalizer.normalize(rows)).reconcile(schemas.AdAccounts)
        row = reconciled.iloc[0]
        assert (row["owner_id"], row["owner_username"]) == ("9", "brand")
        assert json.loads(row["permissions"]) == ["ADMIN"]


class _FakeReportApi:
    """A Pinterest analytics API that finishes each report after one ``IN_PROGRESS`` poll."""

    def __init__(self, final: dict[str, Any], download: bytes = b"") -> None:
        self.final = final
        self.download = download
        self.created: list[dict[str, Any]] = []
        self.polls = 0

    def api(self, request: httpx2.Request) -> httpx2.Response:
        assert request.url.path == "/v5/ad_accounts/549759962542/reports"
        if request.method == "POST":
            self.created.append(json.loads(request.content))
            return httpx2.Response(200, json={"token": "tok", "report_status": "IN_PROGRESS"})
        assert request.url.params["token"] == "tok"
        self.polls += 1
        if self.polls == 1:
            return httpx2.Response(200, json={"report_status": "IN_PROGRESS", "url": None})
        return httpx2.Response(200, json=self.final)

    def storage(self, request: httpx2.Request) -> httpx2.Response:
        assert request.extensions["timeout"]["read"] == constants.DOWNLOAD_TIMEOUT.read
        assert "Authorization" not in request.headers
        return httpx2.Response(200, content=self.download)


@pytest.fixture
def fake_api(monkeypatch: Any):
    def install(final: dict[str, Any], download: bytes = b"") -> tuple[Any, _FakeReportApi]:
        api = _FakeReportApi(final, download)
        real_client = httpx2.AsyncClient
        monkeypatch.setattr(source_module, "_POLL_INTERVAL", 0)
        monkeypatch.setattr(
            source_module.httpx2,
            "AsyncClient",
            lambda **kwargs: real_client(transport=httpx2.MockTransport(api.storage), **kwargs),
        )
        return _source_with_api(api.api), api

    return install


class TestReportFlow:
    async def test_rows_are_flattened_across_entities(self, fake_api):
        payload = {"687001": [{"DATE": "2026-09-14", "AD_ID": "687001"}], "687002": [{"DATE": "2026-09-14"}]}
        source, api = fake_api(
            {"report_status": "FINISHED", "url": "https://storage.example/r.json"},
            json.dumps(payload).encode(),
        )
        rows = await source._report(DAY, level="PIN_PROMOTION", columns=["AD_ID"])
        assert rows == [{"DATE": "2026-09-14", "AD_ID": "687001"}, {"DATE": "2026-09-14"}]
        assert api.polls == 2
        (body,) = api.created
        assert body == {
            "start_date": "2026-09-14",
            "end_date": "2026-09-14",
            "granularity": "DAY",
            "level": "PIN_PROMOTION",
            "columns": ["AD_ID"],
            "report_format": "JSON",
        }

    async def test_targeting_types_are_sent_only_when_given(self, fake_api):
        source, api = fake_api({"report_status": "FINISHED", "url": None})
        rows = await source._report(
            DAY,
            level="PIN_PROMOTION_TARGETING",
            columns=["PIN_PROMOTION_ID"],
            targeting_types=constants.VIDEO_TARGETING_TYPES,
        )
        assert rows == []
        assert api.created[0]["targeting_types"] == ["APPTYPE", "PLACEMENT"]

    async def test_failed_report_raises(self, fake_api):
        source, _ = fake_api({"report_status": "FAILED"})
        with pytest.raises(RuntimeError, match="FAILED"):
            await source._report(DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"])

    async def test_report_not_ready_in_time_raises(self, fake_api, monkeypatch: Any):
        source, api = fake_api({"report_status": "IN_PROGRESS"})
        monkeypatch.setattr(source_module, "_REPORT_TIMEOUT", 0)
        with pytest.raises(RuntimeError, match="not ready within 0s"):
            await source._report(DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"])
        assert api.polls == 1

    async def test_empty_download_yields_no_rows(self, fake_api):
        source, _ = fake_api({"report_status": "FINISHED", "url": "https://storage.example/r.json"})
        assert await source._report(DAY, level="CAMPAIGN", columns=["CAMPAIGN_ID"]) == []

    @pytest.mark.parametrize(
        ("key", "level", "columns", "targeting_types"),
        [
            ("ads_stats", "PIN_PROMOTION", [*constants.AD_METRICS, "AD_ID"], None),
            ("campaigns_stats", "CAMPAIGN", constants.CAMPAIGN_METRICS, None),
            ("ads_conversions_stats", "PIN_PROMOTION", constants.ADS_CONVERSIONS_METRICS, None),
            (
                "videos_stats_by_targeting",
                "PIN_PROMOTION_TARGETING",
                constants.VIDEOS_METRICS,
                constants.VIDEO_TARGETING_TYPES,
            ),
        ],
    )
    async def test_asset_requests_its_level_and_columns(
        self, fake_api, key: str, level: str, columns: list[str], targeting_types: list[str] | None
    ):
        source, api = fake_api({"report_status": "FINISHED", "url": None})
        assert await _run(source, key) == []
        (body,) = api.created
        assert body["level"] == level
        assert body["columns"] == columns
        assert body.get("targeting_types") == targeting_types


class TestEntityListing:
    async def test_list_follows_the_bookmark_and_stamps_the_date(self):
        pages = {
            None: {"items": [{"id": "1"}, {"id": "2"}], "bookmark": "b2"},
            "b2": {"items": [{"id": "3"}], "bookmark": None},
        }
        paths: set[str] = set()

        def handler(request: httpx2.Request) -> httpx2.Response:
            paths.add(request.url.path)
            return httpx2.Response(200, json=pages[request.url.params.get("bookmark")])

        rows = await _source_with_api(handler)._list("ads", DAY)
        assert rows == [{"id": "1", "date": DAY}, {"id": "2", "date": DAY}, {"id": "3", "date": DAY}]
        assert paths == {"/v5/ad_accounts/549759962542/ads"}

    async def test_ad_accounts_fetches_the_sources_account(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"id": "549759962542", "name": "Brand", "currency": "EUR"})

        rows = await _run(_source_with_api(handler), "ad_accounts")
        assert rows == [{"id": "549759962542", "name": "Brand", "currency": "EUR", "date": DAY}]
        assert [request.url.path for request in seen] == ["/v5/ad_accounts/549759962542"]

    @pytest.mark.parametrize("key", ["campaigns", "ad_groups", "ads"])
    async def test_listing_assets_page_through_their_resource(self, key: str):
        seen: list[httpx2.URL] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request.url)
            return httpx2.Response(200, json={"items": [{"id": "1"}], "bookmark": None})

        assert await _run(_source_with_api(handler), key) == [{"id": "1", "date": DAY}]
        assert [url.path for url in seen] == [f"/v5/ad_accounts/549759962542/{key}"]
        assert seen[0].params["page_size"] == str(constants.PAGE_SIZE)
