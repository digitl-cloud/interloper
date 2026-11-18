"""Regression tests for the SnapchatAds source.

Stats rows nest metrics under a ``stats`` dict (flattened and de-prefixed) or,
for dimensioned reports, a ``dimension_stats`` list (exploded into one row per
dimension) — reshaped by ``SnapchatStatsNormalizer``. Entity records are
flattened by a configured ``DataFrameNormalizer``. These tests pin which
normalizer each asset gets, that the normalizers survive the host→child spec round-trip (the AmazonAds
failure mode), that results reconcile against the ported schemas, and what
every asset sends and returns against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.snapchat_ads import constants
from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.connector import schemas
from interloper_assets.snapchat_ads.connector.normalizer import SnapchatStatsNormalizer
from interloper_assets.snapchat_ads.connector.source import SnapchatAds


def _source() -> Any:
    return SnapchatAds(
        id="src-1",
        account_id="acc-1",
        connection=SnapchatAdsConnection(client_id="x", client_secret="x", refresh_token="x"),
    )


class TestSource:
    def test_eight_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "ads_stats",
            "campaigns_stats",
            "ads_stats_by_country",
            "videos_stats_by_os",
            "ad_accounts",
            "ads",
            "ad_squads",
            "campaigns",
        }

    def test_stats_assets_use_stats_normalizer(self):
        by_key = {type(a).key: a for a in _source().assets}
        for key in ("ads_stats", "campaigns_stats", "ads_stats_by_country", "videos_stats_by_os"):
            assert isinstance(by_key[key].normalizer, SnapchatStatsNormalizer), key

    def test_entity_assets_use_flattening_normalizer(self):
        by_key = {type(a).key: a for a in _source().assets}
        for key in ("ads", "ad_squads", "campaigns", "ad_accounts"):
            norm = by_key[key].normalizer
            assert isinstance(norm, DataFrameNormalizer) and not isinstance(norm, SnapchatStatsNormalizer), key
            assert norm.flatten_max_level == 3
            assert norm.drop_na_columns is True


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_stats_normalizer_survives_roundtrip(self):
        # The custom subclass must reconstruct in the child (not degrade to the base).
        assert isinstance(self._child("ads_stats").normalizer, SnapchatStatsNormalizer)

    def test_entity_normalizer_survives_roundtrip(self):
        norm = self._child("ads").normalizer
        assert isinstance(norm, DataFrameNormalizer) and not isinstance(norm, SnapchatStatsNormalizer)
        assert norm.flatten_max_level == 3 and norm.drop_na_columns is True

    def test_ads_row_conforms_after_roundtrip(self):
        child = self._child("ads_stats")
        rows = [{"id": "ad1", "type": "AD", "stats": {"impressions": "100"}, "date": dt.date(2026, 6, 10)}]
        normalized = child.normalizer.normalize(rows)
        out = TableData.from_data(normalized).reconcile(schemas.AdsStats)
        assert int(out.loc[0, "impressions"]) == 100

    def test_ads_entity_conforms_after_roundtrip(self):
        child = self._child("ads")
        normalized = child.normalizer.normalize([{"id": "ad1", "name": "My Ad"}])
        TableData.from_data(normalized).reconcile(schemas.Ads)  # must not raise


DAY = dt.date(2026, 10, 1)
REPORT_METRICS = constants.CONVERSION_METRICS + constants.CORE_METRICS + constants.ADDITIONAL_METRICS


async def _run(key: str, handler: Callable[[httpx2.Request], httpx2.Response]) -> list[dict[str, Any]]:
    connection = SnapchatAdsConnection(id="sc", client_id="x", client_secret="x", refresh_token="x")
    connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))
    source = SnapchatAds(id="src-1", account_id="acc-1", connection=connection)
    asset = next(a for a in source.assets if type(a).key == key)
    return await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=DAY).window))


def _stats(breakdown: str, rows: list[dict[str, Any]], next_link: str | None = None) -> httpx2.Response:
    total = {"id": "acc-1", "type": "AD_ACCOUNT", "breakdown_stats": {breakdown: rows}}
    body: dict[str, Any] = {"request_status": "SUCCESS", "total_stats": [{"total_stat": total}]}
    if next_link is not None:
        body["paging"] = {"next_link": next_link}
    return httpx2.Response(200, json=body)


def _entities(
    list_key: str, item_key: str, records: list[dict[str, Any]], next_link: str | None = None
) -> httpx2.Response:
    items = [{"sub_request_status": "SUCCESS", item_key: record} for record in records]
    body: dict[str, Any] = {"request_status": "SUCCESS", list_key: items}
    if next_link is not None:
        body["paging"] = {"next_link": next_link}
    return httpx2.Response(200, json=body)


class TestReportRequests:
    @pytest.mark.parametrize(
        ("key", "breakdown", "fields", "report_dimension"),
        [
            ("ads_stats", "ad", REPORT_METRICS, None),
            ("campaigns_stats", "campaign", REPORT_METRICS, None),
            ("ads_stats_by_country", "ad", constants.CONVERSION_METRICS + constants.DELIVERY_METRICS, "country"),
            ("videos_stats_by_os", "ad", constants.VIDEOS_METRICS, "os"),
        ],
    )
    async def test_a_report_totals_its_day_over_the_account_stats(
        self, key: str, breakdown: str, fields: list[str], report_dimension: str | None
    ):
        requests: list[httpx2.Request] = []
        row = {"id": "x-1", "type": breakdown.upper(), "stats": {"impressions": 10}}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _stats(breakdown, [row])

        assert await _run(key, handler) == [{**row, "date": DAY}]
        (request,) = requests
        expected = [
            ("granularity", "TOTAL"),
            ("start_time", "2026-10-01"),
            ("end_time", "2026-10-02"),
            ("breakdown", breakdown),
            ("fields", ",".join(fields)),
        ]
        if report_dimension is not None:
            expected.append(("report_dimension", report_dimension))
        assert request.method == "GET"
        assert str(request.url.copy_with(query=None)) == "https://adsapi.snapchat.com/v1/adaccounts/acc-1/stats"
        assert request.url.params.multi_items() == expected

    async def test_every_total_of_every_page_is_read(self):
        urls: list[str] = []
        next_link = "https://adsapi.snapchat.com/v1/adaccounts/acc-1/stats?cursor=page-2"

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            if "cursor" in request.url.params:
                return _stats("ad", [{"id": "ad-3"}])
            response = _stats("ad", [{"id": "ad-1"}], next_link)
            body = response.json()
            body["total_stats"].append({"total_stat": {"breakdown_stats": {"ad": [{"id": "ad-2"}]}}})
            return httpx2.Response(200, json=body)

        rows = await _run("ads_stats", handler)

        assert rows == [{"id": f"ad-{i}", "date": DAY} for i in (1, 2, 3)]
        assert len(urls) == 2 and urls[1] == next_link

    async def test_a_day_without_stats_returns_no_rows(self):
        assert await _run("ads_stats", lambda _: httpx2.Response(200, json={"request_status": "SUCCESS"})) == []

    async def test_a_refused_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if "cursor" in request.url.params:
                return httpx2.Response(401, json={"request_status": "ERROR", "debug_message": "Unauthorized"})
            return _stats("ad", [{"id": "ad-1"}], "https://adsapi.snapchat.com/v1/adaccounts/acc-1/stats?cursor=2")

        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await _run("ads_stats", handler)


class TestEntityRequests:
    @pytest.mark.parametrize(
        ("key", "path", "list_key", "item_key"),
        [
            ("ad_accounts", "/v1/adaccounts/acc-1", "adaccounts", "adaccount"),
            ("ads", "/v1/adaccounts/acc-1/ads", "ads", "ad"),
            ("ad_squads", "/v1/adaccounts/acc-1/adsquads", "adsquads", "adsquad"),
            ("campaigns", "/v1/adaccounts/acc-1/campaigns", "campaigns", "campaign"),
        ],
    )
    async def test_an_entity_lists_the_account_records_stamped_with_the_day(
        self, key: str, path: str, list_key: str, item_key: str
    ):
        requests: list[httpx2.Request] = []
        record = {"id": "x-1", "name": "Spring", "targeting": {"geos": [{"country_code": "de"}]}}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _entities(list_key, item_key, [record])

        assert await _run(key, handler) == [{**record, "date": DAY}]
        (request,) = requests
        assert request.method == "GET"
        assert str(request.url) == f"https://adsapi.snapchat.com{path}"

    async def test_every_page_is_read_by_its_next_link(self):
        urls: list[str] = []
        next_link = "https://adsapi.snapchat.com/v1/adaccounts/acc-1/campaigns?cursor=page-2&limit=1000"

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(str(request.url))
            if "cursor" in request.url.params:
                return _entities("campaigns", "campaign", [{"id": "c-2"}])
            return _entities("campaigns", "campaign", [{"id": "c-1"}], next_link)

        rows = await _run("campaigns", handler)

        assert rows == [{"id": "c-1", "date": DAY}, {"id": "c-2", "date": DAY}]
        assert urls == ["https://adsapi.snapchat.com/v1/adaccounts/acc-1/campaigns", next_link]

    async def test_an_empty_list_returns_no_rows(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"request_status": "SUCCESS", "paging": {}})

        assert await _run("ads", handler) == []
        assert len(requests) == 1

    async def test_a_refused_request_raises(self):
        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _run("ads", lambda _: httpx2.Response(403, json={"request_status": "ERROR"}))
