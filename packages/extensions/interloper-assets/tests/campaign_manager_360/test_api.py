"""Tests for the Campaign Manager 360 (DFA Reporting) API engine.

Every request goes through Google's discovery client, one client per call since
its transport is not thread-safe. A report is a one-shot definition: inserted,
run, polled until its file is available, downloaded as CSV and deleted. These
tests run the engine on a real ``dfareporting`` client over a scripted HTTP
exchange, so they pin the requests as sent.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
from typing import Any

import pytest
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpMockSequence

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

_Answer = tuple[dict[str, str], str | bytes]


def _json(body: dict[str, Any]) -> _Answer:
    return {"status": "200"}, json.dumps(body)


def _refused(status: int, message: str) -> _Answer:
    return {"status": str(status)}, json.dumps({"error": {"code": status, "message": message}})


_DELETED: _Answer = ({"status": "204"}, "")
_POLL = ("GET", f"{_BASE}/userprofiles/111/reports/1/files/2?alt=json", None)


def _report_answers(*statuses: str, file: bytes = _FILE) -> list[_Answer]:
    return [
        _json({"id": "1"}),
        _json({"id": "2"}),
        *(_json({"status": status}) for status in statuses),
        ({"status": "200"}, file),
        _DELETED,
    ]


class Discovery:
    """Build real ``dfareporting`` clients over one scripted HTTP exchange, counting the builds.

    Stands in for the connection's ``client``: every request its clients send is
    recorded on the one HTTP mock, which answers them in order.
    """

    def __init__(self, *answers: _Answer):
        """Script the answers.

        Args:
            *answers: One answer per request, in order: its headers, ``status`` included, and its body.
        """
        self.http = HttpMockSequence(list(answers))
        self.builds = 0

    def __call__(self) -> Any:
        """Build a client on the scripted HTTP, as the connection's ``client`` does.

        Returns:
            A ``dfareporting`` discovery client.
        """
        self.builds += 1
        return build(constants.API_SERVICE, constants.API_VERSION, http=self.http)

    @property
    def requests(self) -> list[tuple[str, str, str | None]]:
        """The requests sent so far: each one's method, URL and body as sent."""
        return [(method, uri, body) for uri, method, body, _ in self.http.request_sequence]


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the engine's waits instead of sleeping them.

    Args:
        monkeypatch: The pytest monkeypatch fixture that restores the engine's clock after the test.

    Returns:
        The waits, in seconds, in the order the engine asked for them.
    """
    slept: list[float] = []
    monkeypatch.setattr(api, "sleep", slept.append)
    return slept


class TestGet:
    def test_reads_the_one_response_of_an_unscoped_method(self):
        discovery = Discovery(_json({"items": [{"profileId": "111"}]}))

        assert CampaignManager360Api(discovery).get("userProfiles.list") == {"items": [{"profileId": "111"}]}
        assert discovery.requests == [("GET", f"{_BASE}/userprofiles?alt=json", None)]

    def test_a_nested_method_takes_the_vendors_parameters(self):
        discovery = Discovery(_json({"status": "REPORT_AVAILABLE"}))

        CampaignManager360Api(discovery).get("reports.files.get", profileId="111", reportId="1", fileId="2")

        assert discovery.requests == [_POLL]

    def test_every_call_builds_its_own_client(self):
        discovery = Discovery(_json({"items": []}), _json({"items": []}))
        engine = CampaignManager360Api(discovery)

        engine.get("userProfiles.list")
        engine.get("userProfiles.list")

        assert discovery.builds == 2

    def test_a_refusal_raises_googles_error(self):
        discovery = Discovery(_refused(403, "The service account has no profile"))

        with pytest.raises(HttpError, match="The service account has no profile"):
            CampaignManager360Api(discovery).get("userProfiles.list")


class TestRows:
    def test_every_page_is_read_on_one_client_scoped_to_the_profile(self):
        discovery = Discovery(
            _json({"remarketingLists": [{"id": "1"}], "nextPageToken": "next"}),
            _json({"remarketingLists": [{"id": "2"}]}),
        )

        rows = CampaignManager360Api(discovery).rows(
            "remarketingLists.list", "remarketingLists", profileId="111", advertiserId="9"
        )

        assert rows == [{"id": "1"}, {"id": "2"}]
        assert discovery.requests == [
            ("GET", f"{_BASE}/userprofiles/111/remarketingLists?advertiserId=9&alt=json", None),
            ("GET", f"{_BASE}/userprofiles/111/remarketingLists?advertiserId=9&pageToken=next&alt=json", None),
        ]
        assert discovery.builds == 1

    def test_a_list_without_records_is_empty_after_one_request(self):
        discovery = Discovery(_json({"kind": "dfareporting#remarketingListsListResponse"}))

        rows = CampaignManager360Api(discovery).rows(
            "remarketingLists.list", "remarketingLists", profileId="111", advertiserId="9"
        )

        assert rows == []
        assert len(discovery.requests) == 1

    def test_a_refusal_on_a_later_page_raises(self):
        discovery = Discovery(
            _json({"remarketingLists": [{"id": "1"}], "nextPageToken": "next"}),
            _refused(429, "Quota exceeded"),
        )

        with pytest.raises(HttpError, match="Quota exceeded"):
            CampaignManager360Api(discovery).rows(
                "remarketingLists.list", "remarketingLists", profileId="111", advertiserId="9"
            )


class TestReport:
    def test_runs_one_day_of_a_standard_report_and_reads_its_file(
        self, sleeps: list[float], caplog: pytest.LogCaptureFixture
    ):
        caplog.set_level(logging.INFO, logger=api.__name__)
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE"))

        records = CampaignManager360Api(discovery).report(
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
        assert discovery.requests == [
            ("POST", f"{_BASE}/userprofiles/111/reports?alt=json", json.dumps(definition)),
            ("POST", f"{_BASE}/userprofiles/111/reports/1/run?alt=json", None),
            _POLL,
            ("GET", f"{_BASE}/reports/1/files/2?alt=media", None),
            ("DELETE", f"{_BASE}/userprofiles/111/reports/1?", None),
        ]
        assert discovery.builds == 1
        assert sleeps == []
        assert [record.message for record in caplog.records if record.name == api.__name__] == [
            "Report id: 1 (STANDARD)",
            "Waiting for report 1 (file 2)...",
            "Downloading report 1: 0%",
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

    def test_a_reach_report_holds_its_criteria_under_reach_criteria(self):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE"))

        CampaignManager360Api(discovery).report(
            "111",
            _DAY,
            account_id="222",
            report_type="REACH",
            dimensions=["campaign"],
            metrics=["uniqueReachTotalReach"],
        )

        definition = json.loads(discovery.requests[0][2] or "")
        assert definition["name"] == "report_reach_2026-10-01_2026-10-01"
        assert definition["type"] == "REACH"
        assert definition["reachCriteria"] == {
            "dateRange": {"startDate": "2026-10-01", "endDate": "2026-10-01"},
            "dimensions": [{"name": "campaign"}],
            "metricNames": ["uniqueReachTotalReach"],
        }
        assert "criteria" not in definition

    def test_further_criteria_join_the_dimensions_and_metrics(self):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE"))
        filters = [{"dimensionName": "advertiserId", "value": "9"}]

        CampaignManager360Api(discovery).report(
            "111",
            _DAY,
            account_id="222",
            report_type="STANDARD",
            dimensions=["campaign"],
            metrics=["impressions"],
            dimensionFilters=filters,
        )

        assert json.loads(discovery.requests[0][2] or "")["criteria"]["dimensionFilters"] == filters

    def test_a_report_type_of_another_shape_is_refused_before_any_request(self):
        discovery = Discovery()

        with pytest.raises(KeyError, match="PATH_TO_CONVERSION"):
            CampaignManager360Api(discovery).report(
                "111", _DAY, account_id="222", report_type="PATH_TO_CONVERSION", dimensions=[], metrics=[]
            )
        assert discovery.builds == 0

    def test_a_pending_file_is_polled_with_doubling_waits_up_to_the_cap(self, sleeps: list[float]):
        pending = ["QUEUED", "PROCESSING", "PROCESSING", "PROCESSING", "PROCESSING", "PROCESSING", "PROCESSING"]
        discovery = Discovery(*_report_answers(*pending, "REPORT_AVAILABLE"))

        CampaignManager360Api(discovery).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert sleeps == [60.0, 120.0, 240.0, 480.0, 960.0, 1200.0, 1200.0]
        assert discovery.requests[2:10] == [_POLL] * 8

    @pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])
    def test_a_failed_report_raises_and_its_definition_is_still_deleted(self, status: str):
        discovery = Discovery(_json({"id": "1"}), _json({"id": "2"}), _json({"status": status}), _DELETED)

        with pytest.raises(RuntimeError, match=rf"^Report 1 failed with status {status}$"):
            CampaignManager360Api(discovery).report(
                "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
            )
        assert discovery.requests[2:] == [_POLL, ("DELETE", f"{_BASE}/userprofiles/111/reports/1?", None)]

    def test_a_file_past_its_deadline_raises_and_its_definition_is_still_deleted(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        clock = iter([0.0, 3599.0, constants.REPORT_TIMEOUT])
        monkeypatch.setattr(api, "monotonic", lambda: next(clock))
        discovery = Discovery(
            _json({"id": "1"}), _json({"id": "2"}), _json({"status": "QUEUED"}), _json({"status": "QUEUED"}), _DELETED
        )

        with pytest.raises(RuntimeError, match=r"^Report 1 did not complete within 10800s$"):
            CampaignManager360Api(discovery).report(
                "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
            )
        assert sleeps == [60.0]
        assert discovery.requests[2:] == [_POLL, _POLL, ("DELETE", f"{_BASE}/userprofiles/111/reports/1?", None)]

    def test_a_refused_definition_raises_and_nothing_more_is_sent(self):
        discovery = Discovery(_refused(403, "No access to account 222"))

        with pytest.raises(HttpError, match="No access to account 222"):
            CampaignManager360Api(discovery).report(
                "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
            )
        assert len(discovery.requests) == 1

    def test_a_failed_cleanup_is_logged_and_the_records_kept(self, caplog: pytest.LogCaptureFixture):
        answers = _report_answers("REPORT_AVAILABLE")
        answers[-1] = _refused(500, "Backend error")
        discovery = Discovery(*answers)

        records = CampaignManager360Api(discovery).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert len(records) == 2
        assert [(record.name, record.levelname) for record in caplog.records if "delete" in record.message] == [
            (api.__name__, "WARNING")
        ]
        assert "Failed to delete report 1: <HttpError 500" in caplog.text

    @pytest.mark.parametrize(
        "file",
        [
            b"Report Name,x\n\nReport Fields\nDate,Impressions\nGrand Total:,0\n",
            b"Report Name,x\n\nReport Fields\nDate,Impressions\n",
        ],
        ids=["totals only", "headers only"],
    )
    def test_a_report_of_nothing_has_no_records(self, file: bytes):
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE", file=file))

        records = CampaignManager360Api(discovery).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert records == []

    def test_a_file_without_totals_keeps_every_row(self):
        file = _FILE.replace(b"Grand Total:,,300,85.5\n", b"")
        discovery = Discovery(*_report_answers("REPORT_AVAILABLE", file=file))

        records = CampaignManager360Api(discovery).report(
            "111", _DAY, account_id="222", report_type="STANDARD", dimensions=["date"], metrics=["impressions"]
        )

        assert [record["Campaign"] for record in records] == ["Campaign A", "Campaign B"]
