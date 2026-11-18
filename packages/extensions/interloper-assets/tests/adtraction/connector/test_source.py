"""Tests for the Adtraction source.

The program is a snapshot of the advertiser account. Each report is one POST
for one day counting approved and pending transactions, and the transactions
are three POSTs for one Stockholm day, one per status Adtraction lists apart.
These tests pin the requests each asset sends, the rows it returns, the
``date`` it stamps on rows that carry no day, and how a vendor transaction
lands on the schema.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.representation import TableData

from interloper_assets.adtraction.connection import AdtractionConnection
from interloper_assets.adtraction.connector.source import Adtraction

DAY = dt.date(2026, 10, 1)
BASE_URL = "https://api.adtraction.net/v2"
REPORT_BODY = {"fromDate": "2026-10-01", "toDate": "2026-10-01", "transactionStatus": 3}
WINDOW = {"fromDate": "2026-10-01T00:00:00+0200", "toDate": "2026-10-01T23:59:59+0200"}

TRANSACTION = {
    "click": {
        "adId": 26503038,
        "adName": "980x120 summer sale",
        "adType": "Image",
        "channelId": 1062755777,
        "channelName": "Example blog",
        "clickDate": "2026-09-30T21:15:00+0200",
        "status": "Fresh",
    },
    "cost": 15,
    "currency": "SEK",
    "paymentStatus": 1,
    "orderId": "abc123_xyz",
    "orderValue": 1000,
    "transactionDate": "2026-10-01T08:30:00+0200",
    "autoApprovalDate": "2026-12-31",
    "transactionName": "Order new customer",
    "transactionStatus": 2,
    "uniqueId": "25862d1e-dc51-4fb2-82ae-24b8533ef417",
    "device": "smartphone",
    "operatingSystem": "android",
    "geoLocation": "SE",
    "productCategoryData": [{"categoryId": "widgets", "categoryName": "Green widgets", "orderValue": 1000, "cost": 15}],
}

METRICS = {
    "clicks": 150,
    "clicksUnique": 100,
    "cost": 600,
    "impressions": 1500,
    "impressionsUnique": 1300,
    "orderValue": 1000.1,
    "leads": 20,
    "sales": 10,
    "ctr": 0.06666,
    "leadRatio": 0.021,
    "conversionRatio": 0.032,
    "aov": 100.87,
}

ENTITY_ASSETS = ["programs"]
REPORT_ASSETS = ["program_stats", "ads_stats", "channels_stats", "segments_stats", "transactions"]


def _source(handler: Callable[[httpx2.Request], httpx2.Response]) -> Adtraction:
    connection = AdtractionConnection(id="adtraction", api_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return Adtraction(id="src-1", connection=connection)


def _asset(source: Adtraction, key: str) -> Any:
    return next(asset for asset in source.assets if type(asset).key == key)


async def _run(key: str, answer: Callable[[httpx2.Request], Any]) -> tuple[list[dict[str, Any]], list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json=answer(request))

    asset = _asset(_source(handler), key)
    rows = await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=DAY).window))
    return rows, requests


def _sent(request: httpx2.Request) -> tuple[str, str, dict[str, Any] | None]:
    body = json.loads(request.content) if request.content else None
    return request.method, str(request.url), body


class TestPartitioning:
    def test_the_program_is_a_snapshot_entity(self):
        for key in ENTITY_ASSETS:
            asset = _asset(_source(lambda _: httpx2.Response(200)), key)
            assert isinstance(asset.partitioning, il.SnapshotPartitionConfig), key
            assert asset.partitioning.column == "date", key
            assert asset.tags == ["Entity"], key

    def test_reports_are_time_partitioned_on_date(self):
        for key in REPORT_ASSETS:
            asset = _asset(_source(lambda _: httpx2.Response(200)), key)
            assert isinstance(asset.partitioning, il.TimePartitionConfig), key
            assert asset.partitioning.column == "date", key
            assert asset.tags == ["Report"], key


class TestRequests:
    async def test_programs_read_the_advertiser_account_and_stamp_the_snapshot_day(self):
        account = {"programId": 52561763, "programName": "Example program", "currency": "SEK", "market": "SE"}

        rows, requests = await _run("programs", lambda _: account)

        assert rows == [{**account, "date": DAY}]
        assert [_sent(request) for request in requests] == [("GET", f"{BASE_URL}/advertiser/account/", None)]

    async def test_program_stats_keep_the_day_adtraction_reports(self):
        row = {"date": "2026-10-01", "clicks": 150, "cost": 600}

        rows, requests = await _run("program_stats", lambda _: [row])

        assert rows == [row]
        assert [_sent(request) for request in requests] == [
            ("POST", f"{BASE_URL}/advertiser/statistics/days/", REPORT_BODY)
        ]

    @pytest.mark.parametrize(
        ("key", "path", "row"),
        [
            ("ads_stats", "/advertiser/statistics/ads/", {"adId": 1062755777, "clicks": 150}),
            ("channels_stats", "/advertiser/statistics/channels/", {"channelId": 1062755777, "clicks": 150}),
            ("segments_stats", "/advertiser/statistics/segments/", {"segmentId": 1104616062, "clicks": 150}),
        ],
    )
    async def test_breakdowns_ask_for_the_day_and_stamp_it(self, key, path, row):
        rows, requests = await _run(key, lambda _: [row])

        assert rows == [{**row, "date": DAY}]
        assert [_sent(request) for request in requests] == [("POST", f"{BASE_URL}{path}", REPORT_BODY)]

    async def test_transactions_list_every_status_for_the_stockholm_day_and_stamp_it(self):
        def answer(request: httpx2.Request) -> Any:
            status = json.loads(request.content)["transactionStatus"]
            if status == 4:
                return {"message": "No transactions found.", "status": 200}
            return [{"orderId": f"o-{status}", "transactionStatus": 1 if status == 3 else status}]

        rows, requests = await _run("transactions", answer)

        assert rows == [
            {"orderId": "o-3", "transactionStatus": 1, "date": DAY},
            {"orderId": "o-5", "transactionStatus": 5, "date": DAY},
        ]
        assert [_sent(request) for request in requests] == [
            ("POST", f"{BASE_URL}/advertiser/transactions/", {**WINDOW, "transactionStatus": status})
            for status in (3, 4, 5)
        ]


class TestShape:
    def test_a_transaction_lands_flat_and_typed(self):
        asset = _asset(_source(lambda _: httpx2.Response(200)), "transactions")
        rows = [{**TRANSACTION, "date": DAY}]

        table = TableData.from_data(asset.normalizer.normalize(rows)).reconcile(asset.schema)
        (row,) = table.astype(object).where(table.notna(), None).to_dict("records")

        assert row["click_ad_id"] == "26503038"
        assert row["click_channel_id"] == "1062755777"
        assert row["click_click_date"] == dt.datetime(2026, 9, 30, 19, 15, tzinfo=dt.UTC)
        assert row["transaction_date"] == dt.datetime(2026, 10, 1, 6, 30, tzinfo=dt.UTC)
        assert row["auto_approval_date"] == dt.date(2026, 12, 31)
        assert row["transaction_status"] == 2
        assert row["cost"] == 15.0
        assert json.loads(row["product_category_data"]) == TRANSACTION["productCategoryData"]
        assert row["reason_text"] is None
        assert row["date"] == DAY

    def test_program_stats_land_on_the_day_adtraction_reports(self):
        asset = _asset(_source(lambda _: httpx2.Response(200)), "program_stats")

        table = TableData.from_data(asset.normalizer.normalize([{"date": "2026-10-01", **METRICS}])).reconcile(
            asset.schema
        )
        (row,) = table.astype(object).where(table.notna(), None).to_dict("records")

        assert row["date"] == DAY
        assert row["clicks_unique"] == 100
        assert row["order_value"] == 1000.1

    @pytest.mark.parametrize(
        ("key", "documented"),
        [
            ("programs", {"programId": 52561763, "programName": "Example", "currency": "SEK", "market": "SE"}),
            ("program_stats", {"date": "2026-10-01", **METRICS}),
            ("ads_stats", {"adId": 1062755777, "adName": "980x120 summer sale", **METRICS}),
            (
                "channels_stats",
                {"channelId": 1062755777, "channelName": "Example blog", "channelType": "Blog", **METRICS},
            ),
            ("segments_stats", {"segmentId": 1104616062, "segmentName": "Gold", **METRICS}),
            (
                "transactions",
                {**TRANSACTION, "reasonText": "Order cancelled", "lastUpdated": "2026-10-02T10:00:00+0200"},
            ),
        ],
    )
    def test_every_documented_field_is_declared_and_nothing_else(self, key, documented):
        asset = _asset(_source(lambda _: httpx2.Response(200)), key)

        columns = set(asset.normalizer.normalize([{"date": DAY, **documented}]).columns)

        assert columns == set(asset.schema.model_fields)
