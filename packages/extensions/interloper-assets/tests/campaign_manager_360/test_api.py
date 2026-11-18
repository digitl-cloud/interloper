"""Tests for the Campaign Manager 360 (DFA Reporting) API engine.

A report is a one-shot definition: inserted, run, polled until its file is
available, downloaded as CSV and deleted. These tests run the engine over a
scripted HTTP exchange, so they pin the requests as sent.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.campaign_manager_360 import api, constants
from interloper_assets.campaign_manager_360.api import CampaignManager360Api

_BASE = "https://dfareporting.googleapis.com/dfareporting/v5"
_DAY = dt.date(2026, 10, 1)

_FILE = b"""Report Name,report_standard_2026-10-01_2026-10-01
Date Range,2026-10-01 - 2026-10-01

Report Fields
Date,Campaign,Impressions,Active View: % Viewable Impressions
2026-10-01,Campaign A,100,85.5
2026-10-01,Campaign B,200,-
Grand Total:,,300,85.5
"""

_Answer = tuple[int, bytes]


def _json(body: dict[str, Any]) -> _Answer:
    return 200, json.dumps(body).encode()


def _refused(status: int, message: str) -> _Answer:
    return status, json.dumps({"error": {"code": status, "message": message}}).encode()


_DELETED: _Answer = (204, b"")
_POLL = ("GET", f"{_BASE}/userprofiles/111/reports/1/files/2", None)
_DOWNLOAD = ("GET", f"{_BASE}/reports/1/files/2?alt=media", None)
_DELETE = ("DELETE", f"{_BASE}/userprofiles/111/reports/1", None)


def _report_answers(*statuses: str, file: bytes = _FILE) -> list[_Answer]:
    return [
        _json({"id": "1"}),
        _json({"id": "2"}),
        *(_json({"status": status}) for status in statuses),
        (200, file),
        _DELETED,
    ]


class FakeApi:
    """Answer the API's requests from a script, in order, recording each one's method, URL and JSON body."""

    def __init__(self, *answers: _Answer):
        """Script the answers.

        Args:
            *answers: One answer per request, in order: its status and its body.
        """
        self.answers = list(answers)
        self.requests: list[tuple[str, str, Any]] = []
        self.client = il.AsyncRESTClient(
            constants.BASE_URL, transport=httpx2.MockTransport(self.answer), retry=il.RetryPolicy(max_attempts=1)
        )

    def answer(self, request: httpx2.Request) -> httpx2.Response:
        """Record one request and serve the next answer.

        Args:
            request: The request.

        Returns:
            The next scripted answer.
        """
        self.requests.append(
            (request.method, str(request.url), json.loads(request.content) if request.content else None)
        )
        status, body = self.answers.pop(0)
        return httpx2.Response(status, content=body)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the engine's waits instead of sleeping them.

    Args:
        monkeypatch: The pytest monkeypatch fixture that restores the engine's clock after the test.

    Returns:
        The waits, in seconds, in the order the engine asked for them.
    """
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(api, "sleep", sleep)
    return slept


class TestGet:
    async def test_reads_the_one_response_of_an_endpoint(self):
        server = FakeApi(_json({"items": [{"profileId": "111"}]}))

        assert await CampaignManager360Api(server.client).get("/userprofiles") == {"items": [{"profileId": "111"}]}
        assert server.requests == [("GET", f"{_BASE}/userprofiles", None)]

    async def test_sends_the_query_parameters(self):
        server = FakeApi(_json({"advertisers": []}))

        await CampaignManager360Api(server.client).get("/userprofiles/111/advertisers", ids="9")

        assert server.requests == [("GET", f"{_BASE}/userprofiles/111/advertisers?ids=9", None)]

    async def test_a_refusal_raises_its_status(self):
        server = FakeApi(_refused(403, "The service account has no profile"))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await CampaignManager360Api(server.client).get("/userprofiles")

        assert raised.value.response.status_code == 403


_LISTS = "/userprofiles/111/remarketingLists"


class TestRows:
    async def test_every_page_is_read_with_the_query_parameters(self):
        server = FakeApi(
            _json({"remarketingLists": [{"id": "1"}], "nextPageToken": "next"}),
            _json({"remarketingLists": [{"id": "2"}]}),
        )

        rows = await CampaignManager360Api(server.client).rows(_LISTS, "remarketingLists", advertiserId="9")

        assert rows == [{"id": "1"}, {"id": "2"}]
        assert server.requests == [
            ("GET", f"{_BASE}{_LISTS}?advertiserId=9", None),
            ("GET", f"{_BASE}{_LISTS}?advertiserId=9&pageToken=next", None),
        ]

    async def test_a_list_without_records_is_empty_after_one_request(self):
        server = FakeApi(_json({"kind": "dfareporting#remarketingListsListResponse"}))

        rows = await CampaignManager360Api(server.client).rows(_LISTS, "remarketingLists", advertiserId="9")

        assert rows == []
        assert len(server.requests) == 1

    async def test_a_refusal_on_a_later_page_raises(self):
        server = FakeApi(
            _json({"remarketingLists": [{"id": "1"}], "nextPageToken": "next"}),
            _refused(429, "Quota exceeded"),
        )

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await CampaignManager360Api(server.client).rows(_LISTS, "remarketingLists", advertiserId="9")

        assert raised.value.response.status_code == 429


class TestReport:
    async def test_runs_one_day_of_a_standard_report_and_reads_its_file(
        self, sleeps: list[float], caplog: pytest.LogCaptureFixture
    ):
        caplog.set_level(logging.INFO, logger=api.__name__)
        server = FakeApi(*_report_answers("REPORT_AVAILABLE"))

        records = await CampaignManager360Api(server.client).report(
            "111",
            _DAY,
            account_id="222",
            report_type="STANDARD",
            dimensions=["date", "campaign"],
            metrics=["impressions", "activeViewPercentageViewableImpressions"],
        )

        definition = {
            "accountId": "222",
            "name": "report_standard_2026-10-01_2026-10-01",
            "type": "STANDARD",
            "criteria": {
                "dateRange": {"startDate": "2026-10-01", "endDate": "2026-10-01"},
                "dimensions": [{"name": "date"}, {"name": "campaign"}],
                "metricNames": ["impressions", "activeViewPercentageViewableImpressions"],
            },
        }
        assert server.requests == [
            ("POST", f"{_BASE}/userprofiles/111/reports", definition),
            ("POST", f"{_BASE}/userprofiles/111/reports/1/run", None),
            _POLL,
            _DOWNLOAD,
            _DELETE,
        ]
        assert sleeps == []
        assert [record.message for record in caplog.records if record.name == api.__name__] == [
            "Report id: 1 (STANDARD)",
            "Waiting for report 1 (file 2)...",
            "Downloading report 1 (file 2)...",
        ]
        assert records[0] == {
            "Date": "2026-10-01",
            "Campaign": "Campaign A",
            "Impressions": 100,
            "Active View: % Viewable Impressions": 85.5,
        }
        assert records[1]["Campaign"] == "Campaign B"
        assert math.isnan(records[1]["Active View: % Viewable Impressions"])
        assert len(records) == 2

    async def test_a_reach_report_holds_its_criteria_under_reach_criteria(self):
        server = FakeApi(*_report_answers("REPORT_AVAILABLE"))

        await CampaignManager360Api(server.client).report(
            "111",
            _DAY,
            account_id="222",
            report_type="REACH",
            dimensions=["campaign"],
            metrics=["uniqueReachTotalReach"],
        )

        definition = server.requests[0][2]
        assert definition["name"] == "report_reach_2026-10-01_2026-10-01"
        assert definition["type"] == "REACH"
        assert definition["reachCriteria"] == {
            "dateRange": {"startDate": "2026-10-01", "endDate": "2026-10-01"},
            "dimensions": [{"name": "campaign"}],
            "metricNames": ["uniqueReachTotalReach"],
        }
        assert "criteria" not in definition

    async def test_further_criteria_join_the_dimensions_and_metrics(self):
        server = FakeApi(*_report_answers("REPORT_AVAILABLE"))
        filters = [{"dimensionName": "advertiserId", "value": "9"}]

        await CampaignManager360Api(server.client).report(
            "111",
            _DAY,
            account_id="222",
            report_type="STANDARD",
            dimensions=["campaign"],
            metrics=["impressions"],
            dimensionFilters=filters,
        )

        assert server.requests[0][2]["criteria"]["dimensionFilters"] == filters

    async def test_a_report_type_of_another_shape_is_refused_before_any_request(self):
        server = FakeApi()

        with pytest.raises(KeyError, match="PATH_TO_CONVERSION"):
            await CampaignManager360Api(server.client).report(
                "111", _DAY, account_id="222", report_type="PATH_TO_CONVERSION", dimensions=[], metrics=[]
            )
        assert server.requests == []

    async def test_a_pending_file_is_polled_with_doubling_waits_up_to_the_cap(self, sleeps: list[float]):
        pending = ["QUEUED", "PROCESSING", "PROCESSING", "PROCESSING", "PROCESSING", "PROCESSING", "PROCESSING"]
        server = FakeApi(*_report_answers(*pending, "REPORT_AVAILABLE"))

        await CampaignManager360Api(server.client).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert sleeps == [60.0, 120.0, 240.0, 480.0, 960.0, 1200.0, 1200.0]
        assert server.requests[2:10] == [_POLL] * 8

    @pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])
    async def test_a_failed_report_raises_and_its_definition_is_still_deleted(self, status: str):
        server = FakeApi(_json({"id": "1"}), _json({"id": "2"}), _json({"status": status}), _DELETED)

        with pytest.raises(RuntimeError, match=rf"^Report 1 failed with status {status}$"):
            await CampaignManager360Api(server.client).report(
                "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
            )
        assert server.requests[2:] == [_POLL, _DELETE]

    async def test_a_file_past_its_deadline_raises_and_its_definition_is_still_deleted(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        clock = iter([0.0, 3599.0, constants.REPORT_TIMEOUT])
        monkeypatch.setattr(api, "monotonic", lambda: next(clock))
        server = FakeApi(
            _json({"id": "1"}), _json({"id": "2"}), _json({"status": "QUEUED"}), _json({"status": "QUEUED"}), _DELETED
        )

        with pytest.raises(RuntimeError, match=r"^Report 1 did not complete within 10800s$"):
            await CampaignManager360Api(server.client).report(
                "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
            )
        assert sleeps == [60.0]
        assert server.requests[2:] == [_POLL, _POLL, _DELETE]

    async def test_a_refused_definition_raises_and_nothing_more_is_sent(self):
        server = FakeApi(_refused(403, "No access to account 222"))

        with pytest.raises(httpx2.HTTPStatusError):
            await CampaignManager360Api(server.client).report(
                "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
            )
        assert len(server.requests) == 1

    async def test_a_failed_cleanup_is_logged_and_the_records_kept(self, caplog: pytest.LogCaptureFixture):
        answers = _report_answers("REPORT_AVAILABLE")
        answers[-1] = _refused(500, "Backend error")
        server = FakeApi(*answers)

        records = await CampaignManager360Api(server.client).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert len(records) == 2
        assert [(record.name, record.levelname) for record in caplog.records if "delete" in record.message] == [
            (api.__name__, "WARNING")
        ]
        assert "Failed to delete report 1: Server error '500 Internal Server Error'" in caplog.text

    @pytest.mark.parametrize(
        "file",
        [
            b"Report Name,x\n\nReport Fields\nDate,Impressions\nGrand Total:,0\n",
            b"Report Name,x\n\nReport Fields\nDate,Impressions\n",
        ],
        ids=["totals only", "headers only"],
    )
    async def test_a_report_of_nothing_has_no_records(self, file: bytes):
        server = FakeApi(*_report_answers("REPORT_AVAILABLE", file=file))

        records = await CampaignManager360Api(server.client).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert records == []

    async def test_a_file_without_totals_keeps_every_row(self):
        file = _FILE.replace(b"Grand Total:,,300,85.5\n", b"")
        server = FakeApi(*_report_answers("REPORT_AVAILABLE", file=file))

        records = await CampaignManager360Api(server.client).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert [record["Campaign"] for record in records] == ["Campaign A", "Campaign B"]
