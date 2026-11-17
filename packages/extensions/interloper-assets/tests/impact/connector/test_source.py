"""Regression tests for the Impact source.

Impact returns PascalCase fields with digit groups (``SubId1`` -> ``sub_id_1``)
that a source-level ``DataFrameNormalizer(snake_case_digits=True)`` maps to the
schema fields; the source stamps a ``date`` partition column. These tests pin
what each asset sends and returns, that the normalizer reaches every asset,
survives the host→child spec round-trip, and that a raw API-shaped record
conforms to the schema.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.impact.connection import ImpactConnection
from interloper_assets.impact.connector import schemas
from interloper_assets.impact.connector.source import Impact
from interloper_assets.impact.constants import BASE_URL

DAY = dt.date(2026, 6, 10)
ACCOUNT = f"{BASE_URL}/Advertisers/SID"
WINDOW = {"StartDate": "2026-06-10T00:00:00Z", "EndDate": "2026-06-10T23:59:59Z"}


def _source() -> Any:
    return Impact(id="src-1", program_id="123", connection=ImpactConnection(account_sid="x", auth_token="x"))


class _ImpactServer:
    """A fake Impact Advertiser API: a two-page list or a two-page export job.

    Every list and every job download answers ``@numpages`` 2, each page
    holding one record named after its page under ``key``; an export queues
    job ``J1``, which is complete at the first poll.
    """

    def __init__(self, key: str) -> None:
        """Serve records under one key.

        Args:
            key: The key every list page or download page holds its records under.
        """
        self.key = key
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request and record it.

        Args:
            request: The request the engine sent.

        Returns:
            The queued job for an export, the job's status for a poll, or a page of records.
        """
        self.requests.append(request)
        path = request.url.path.removeprefix("/Advertisers/SID")
        if path.endswith("ClickExport") or path.startswith("/ReportExport/"):
            return httpx2.Response(200, json={"Status": "QUEUED", "QueuedUri": "/Advertisers/SID/Jobs/J1"})
        if path == "/Jobs/J1":
            return httpx2.Response(200, json={"Status": "COMPLETED"})
        page = request.url.params["Page"]
        return httpx2.Response(200, json={"@page": page, "@numpages": "2", self.key: [{"Id": page}]})

    def sent(self) -> list[tuple[str, str, dict[str, str], str]]:
        """List the requests the server received.

        Returns:
            Each request's method, URL path, query parameters and ``Accept`` header, in order.
        """
        return [
            (
                request.method,
                str(request.url.copy_with(query=None)),
                dict(request.url.params),
                request.headers["Accept"],
            )
            for request in self.requests
        ]


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> ImpactConnection:
    connection = ImpactConnection(account_sid="SID", auth_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        ACCOUNT,
        auth=httpx2.BasicAuth(username="SID", password="token"),
        headers={"Accept": "application/json"},
        transport=httpx2.MockTransport(handler),
    )
    return connection


async def _run(key: str, handler: Callable[[httpx2.Request], httpx2.Response]) -> list[dict[str, Any]]:
    source = Impact(id="src-1", program_id="123", connection=_connection(handler))
    asset = next(a for a in source.assets if type(a).key == key)
    return await asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=DAY).window))


class TestRequests:
    @pytest.mark.parametrize(
        ("key", "path", "records_key"),
        [
            ("actions", "/Actions", "Actions"),
            ("action_updates", "/ActionUpdates", "ActionUpdates"),
            ("action_inquiries", "/ActionInquiries", "ActionInquiries"),
        ],
    )
    async def test_a_list_reads_every_page_of_the_programs_day(self, key: str, path: str, records_key: str):
        server = _ImpactServer(records_key)

        rows = await _run(key, server)

        assert rows == [{"Id": "1", "date": DAY}, {"Id": "2", "date": DAY}]
        query = {"CampaignId": "123", **WINDOW}
        assert server.sent() == [
            ("GET", f"{ACCOUNT}{path}", {**query, "Page": "1"}, "application/json"),
            ("GET", f"{ACCOUNT}{path}", {**query, "Page": "2"}, "application/json"),
        ]

    @pytest.mark.parametrize(
        ("key", "report_id"),
        [
            ("actions_by_sku", "adv_action_list_sku_pm_only"),
            ("performance_stats", "att_adv_performance_by_day_pm_only"),
            ("performance_stats_by_ad", "adv_performance_by_ad_pm_only"),
            ("performance_stats_by_domain", "att_adv_performance_by_ref_domain_pm_only"),
            ("performance_stats_by_shared_id", "att_adv_performance_by_shared_id_pm_only"),
            ("performance_stats_by_io", "att_adv_performance_by_IO"),
        ],
    )
    async def test_a_report_exports_the_programs_day_and_downloads_every_page(self, key: str, report_id: str):
        server = _ImpactServer("Records")

        rows = await _run(key, server)

        assert rows == [{"Id": "1", "date": DAY}, {"Id": "2", "date": DAY}]
        download = f"{ACCOUNT}/Jobs/J1/Download"
        assert server.sent() == [
            (
                "GET",
                f"{ACCOUNT}/ReportExport/{report_id}",
                {"SUBAID": "123", "ResultFormat": "JSON", **WINDOW},
                "application/json",
            ),
            ("GET", f"{ACCOUNT}/Jobs/J1", {}, "application/json"),
            ("GET", download, {"Mode": "stream", "Page": "1"}, "application/octet-stream"),
            ("GET", download, {"Mode": "stream", "Page": "2"}, "application/octet-stream"),
        ]

    async def test_clicks_export_the_programs_day_and_download_every_page(self):
        server = _ImpactServer("Clicks")

        rows = await _run("clicks", server)

        assert rows == [{"Id": "1", "date": DAY}, {"Id": "2", "date": DAY}]
        download = f"{ACCOUNT}/Jobs/J1/Download"
        assert server.sent() == [
            (
                "GET",
                f"{ACCOUNT}/Programs/123/ClickExport",
                {"Date": "2026-06-10", "ResultFormat": "JSON"},
                "application/json",
            ),
            ("GET", f"{ACCOUNT}/Jobs/J1", {}, "application/json"),
            ("GET", download, {"Mode": "stream", "Page": "1"}, "application/octet-stream"),
            ("GET", download, {"Mode": "stream", "Page": "2"}, "application/octet-stream"),
        ]

    async def test_a_page_without_records_adds_none(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["Page"] == "2":
                return httpx2.Response(200, json={"@numpages": "2", "Actions": None})
            return httpx2.Response(200, json={"@numpages": "2", "Actions": [{"Id": "1"}]})

        assert await _run("actions", handler) == [{"Id": "1", "date": DAY}]

    async def test_a_failed_job_raises_its_status(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("/Jobs/J1"):
                return httpx2.Response(200, json={"Status": "FAILED"})
            return httpx2.Response(200, json={"QueuedUri": "/Advertisers/SID/Jobs/J1"})

        with pytest.raises(RuntimeError, match="Impact job J1 failed with status FAILED"):
            await _run("clicks", handler)

    async def test_a_refused_export_raises_the_http_error(self):
        with pytest.raises(httpx2.HTTPStatusError, match="401 Unauthorized"):
            await _run("performance_stats", lambda _: httpx2.Response(401))


class TestSource:
    def test_all_assets_inherit_the_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key

    def test_ten_report_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "actions",
            "action_updates",
            "action_inquiries",
            "actions_by_sku",
            "clicks",
            "performance_stats",
            "performance_stats_by_ad",
            "performance_stats_by_domain",
            "performance_stats_by_shared_id",
            "performance_stats_by_io",
        }


class TestFraming:
    async def test_an_empty_day_stamps_no_rows(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(200, json={"@numpages": "0", "Actions": []})

        assert await _run("actions", handler) == []

    def test_normalizer_blanks_empty_strings(self):
        # Empty-string blanking is the normalizer's job, opted into on the source.
        norm = _source().normalizer
        assert norm.replace_empty_strings is True
        out = norm.normalize([{"Id": "1", "ReferringDomain": "", "date": DAY}])
        assert pd.isna(out.loc[0, "referring_domain"])


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_actions_record_conforms_after_roundtrip(self):
        child = self._child("actions")
        assert isinstance(child.normalizer, DataFrameNormalizer)

        # A raw PascalCase Actions record; digit-group field exercises snake_case_digits.
        record = {"Id": "A1", "CampaignId": "123", "SubId1": "x", "Amount": "12.50"}
        normalized = child.normalizer.normalize([{**record, "date": DAY}])
        assert {"id", "campaign_id", "sub_id_1", "amount", "date"} <= set(normalized.columns)
        TableData.from_data(normalized).reconcile(schemas.Actions)  # must not raise
