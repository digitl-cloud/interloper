"""Tests for the Display & Video 360 API engine.

Entities are read from the Display & Video API, a list paged by
``nextPageToken``; reports are Bid Manager queries, created, run, polled with
a capped backoff, downloaded as CSV with a summary footer and deleted. Every
method sends its requests on one discovery service of its own, since the
service's transport is not thread-safe. These tests drive the real discovery
services over the ``google`` fake's transport.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import httpx2
import pandas as pd
import pytest
from googleapiclient.errors import HttpError

from interloper_assets.display_video_360 import api as api_module
from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.api import DisplayVideo360Api

_DISPLAY_VIDEO = "https://displayvideo.googleapis.com/v4"
_BID_MANAGER = "https://doubleclickbidmanager.googleapis.com/v2"
_REFUSAL = {"error": {"code": 403, "message": "The caller does not have permission"}}


def _api(google: Any) -> DisplayVideo360Api:
    return DisplayVideo360Api(google.display_video, google.bid_manager)


class TestGet:
    def test_reads_one_object_on_a_service_of_its_own(self, google):
        google.serve({"name": "audience"})

        audience = _api(google).get("firstPartyAndPartnerAudiences", partnerId="1", firstPartyAndPartnerAudienceId="7")

        assert audience == {"name": "audience"}
        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences/7?partnerId=1&alt=json", None)
        ]
        assert google.builds == [constants.DV_API_SERVICE]

    def test_a_nested_resource_is_reached_through_its_parents(self, google):
        google.serve({"lineItemId": "3"})

        line_item = _api(google).get("advertisers.lineItems", advertiserId="9", lineItemId="3")

        assert line_item == {"lineItemId": "3"}
        assert google.requests == [("GET", f"{_DISPLAY_VIDEO}/advertisers/9/lineItems/3?alt=json", None)]

    def test_a_refusal_raises_the_vendors_error(self, google):
        google.serve((403, _REFUSAL))

        with pytest.raises(HttpError, match="The caller does not have permission"):
            _api(google).get("partners", partnerId="1")


class TestRows:
    def test_pages_until_no_next_page_token_on_one_service(self, google):
        google.serve(
            {"firstPartyAndPartnerAudiences": [{"name": "a"}], "nextPageToken": "next"},
            {"firstPartyAndPartnerAudiences": [{"name": "b"}]},
        )

        rows = _api(google).rows("firstPartyAndPartnerAudiences", partnerId="1")

        assert rows == [{"name": "a"}, {"name": "b"}]
        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences?partnerId=1&alt=json", None),
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences?partnerId=1&pageToken=next&alt=json", None),
        ]
        assert google.builds == [constants.DV_API_SERVICE]

    def test_a_page_without_rows_still_pages_on(self, google):
        google.serve({"nextPageToken": "next"}, {"partners": [{"partnerId": "2"}]})

        assert _api(google).rows("partners") == [{"partnerId": "2"}]

    def test_rows_are_read_from_the_pages_one_list(self, google):
        google.serve({"customBiddingScripts": [{"customBiddingScriptId": "4"}]})

        rows = _api(google).rows("customBiddingAlgorithms.scripts", customBiddingAlgorithmId="5", advertiserId="9")

        assert rows == [{"customBiddingScriptId": "4"}]
        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/customBiddingAlgorithms/5/scripts?advertiserId=9&alt=json", None)
        ]

    def test_a_page_of_two_lists_is_refused(self, google):
        google.serve({"partners": [{"partnerId": "1"}], "advertisers": [], "nextPageToken": "next"})

        with pytest.raises(
            RuntimeError,
            match=(
                r"^A page of partners carries more than one field beside nextPageToken: "
                r"\['partners', 'advertisers'\]$"
            ),
        ):
            _api(google).rows("partners")

    def test_an_empty_list_is_no_rows(self, google):
        assert _api(google).rows("partners") == []

    def test_a_refusal_raises_the_vendors_error(self, google):
        google.serve((401, {"error": {"code": 401, "message": "Request had invalid authentication credentials"}}))

        with pytest.raises(HttpError, match="invalid authentication credentials"):
            _api(google).rows("partners")


class FakeClock:
    """Stand in for the engine's ``time``: sleeping moves the clock on instead of waiting."""

    def __init__(self) -> None:
        """Start the clock at zero, with no sleeps."""
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        """Read the clock.

        Returns:
            The seconds slept so far.
        """
        return self.now

    def sleep(self, seconds: float) -> None:
        """Record a wait and move the clock on by it.

        Args:
            seconds: The wait the engine asked for.
        """
        self.sleeps.append(seconds)
        self.now += seconds


def _report(state: str) -> dict[str, Any]:
    return {"metadata": {"status": {"state": state}, "googleCloudStoragePath": "https://storage/report.csv"}}


_QUERY = {"queryId": "q"}
_RUN = {"key": {"reportId": "r"}}
_DELETE = ("DELETE", f"{_BID_MANAGER}/queries/q?", None)


class TestReport:
    @pytest.fixture
    def clock(self, monkeypatch: pytest.MonkeyPatch) -> FakeClock:
        """Give the engine a clock of its own, which sleeps without waiting.

        Args:
            monkeypatch: The pytest monkeypatch fixture that restores the engine's ``time`` after the test.

        Returns:
            The clock, holding the waits the engine asked for, in seconds.
        """
        fake = FakeClock()
        monkeypatch.setattr(api_module, "time", fake)
        return fake

    def _report(self, google: Any, **params: Any) -> list[dict[str, Any]]:
        rows = _api(google).report(dt.date(2026, 7, 10), title="line_items_stats_2026-07-10", **params)
        assert google.builds == [constants.DBM_API_SERVICE]
        return rows

    def test_runs_one_query_over_its_day_and_deletes_it(self, google, download, clock: FakeClock):
        google.serve(_QUERY, _RUN, _report("DONE"))
        params: dict[str, Any] = {
            "type": "STANDARD",
            "groupBys": ["FILTER_DATE"],
            "metrics": ["METRIC_IMPRESSIONS"],
            "filters": [{"type": "FILTER_PARTNER", "value": "1"}],
        }

        rows = self._report(google, **params)

        day = {"year": 2026, "month": 7, "day": 10}
        query = {
            "metadata": {
                "title": "line_items_stats_2026-07-10",
                "dataRange": {"range": "CUSTOM_DATES", "customStartDate": day, "customEndDate": day},
                "format": "CSV",
            },
            "params": params,
            "schedule": {"frequency": "ONE_TIME"},
        }
        assert google.requests == [
            ("POST", f"{_BID_MANAGER}/queries?alt=json", query),
            ("POST", f"{_BID_MANAGER}/queries/q:run?synchronous=false&alt=json", None),
            ("GET", f"{_BID_MANAGER}/queries/q/reports/r?alt=json", None),
            _DELETE,
        ]
        assert download.downloads == [{"url": "https://storage/report.csv", "timeout": None}]
        assert clock.sleeps == []
        assert len(rows) == 2

    def test_the_csv_footer_is_dropped_and_a_dash_is_missing(self, google, download, clock: FakeClock):
        download.text = "Date,Advertiser ID,Impressions,Revenue\n2026/07/10,9,100,-\n2026/07/10,9,200,1.5\n,,300,1.5\n"
        google.serve(_QUERY, _RUN, _report("DONE"))

        rows = self._report(google)

        assert rows[1] == {"Date": "2026/07/10", "Advertiser ID": 9, "Impressions": 200, "Revenue": 1.5}
        assert rows[0]["Impressions"] == 100
        assert pd.isna(rows[0]["Revenue"])

    @pytest.mark.parametrize(
        "text",
        ["Date,Impressions\nNo data returned by the reporting service.,\n", "Date,Impressions\n"],
        ids=["no-data-row", "headers-only"],
    )
    def test_a_report_without_data_is_no_rows(self, google, download, clock: FakeClock, text: str):
        download.text = text
        google.serve(_QUERY, _RUN, _report("DONE"))

        assert self._report(google) == []

    def test_polls_with_a_doubling_wait_capped_at_the_maximum(self, google, download, clock: FakeClock):
        google.serve(_QUERY, _RUN, *[_report("RUNNING")] * 7, _report("DONE"))

        rows = self._report(google)

        assert clock.sleeps == [60, 120, 240, 480, 960, constants.MAX_RETRY_INTERVAL, constants.MAX_RETRY_INTERVAL]
        assert [url for _, url, _ in google.requests].count(f"{_BID_MANAGER}/queries/q/reports/r?alt=json") == 8
        assert len(rows) == 2

    def test_a_failed_report_raises_and_still_deletes_the_query(self, google, clock: FakeClock):
        google.serve(_QUERY, _RUN, _report("FAILED"))

        with pytest.raises(RuntimeError, match=r"^Report r \(query q\) failed$"):
            self._report(google)

        assert google.requests[-1] == _DELETE

    def test_a_report_never_done_times_out_and_still_deletes_the_query(self, google, clock: FakeClock):
        google.serve(_QUERY, _RUN, *[_report("RUNNING")] * 14)

        with pytest.raises(RuntimeError, match=r"^Report r did not complete within 10800s$"):
            self._report(google)

        assert clock.sleeps == [60, 120, 240, 480, 960] + [constants.MAX_RETRY_INTERVAL] * 8
        assert clock.now >= constants.REPORT_TIMEOUT
        assert google.requests[-1] == _DELETE

    def test_a_refused_run_raises_the_vendors_error_and_deletes_the_query(self, google, clock: FakeClock):
        google.serve(_QUERY, (403, _REFUSAL))

        with pytest.raises(HttpError, match="The caller does not have permission"):
            self._report(google)

        assert google.requests[-1] == _DELETE

    def test_a_failed_download_raises_and_deletes_the_query(self, google, download, clock: FakeClock):
        download.status = 403
        google.serve(_QUERY, _RUN, _report("DONE"))

        with pytest.raises(httpx2.HTTPStatusError):
            self._report(google)

        assert google.requests[-1] == _DELETE

    def test_a_failed_delete_is_logged_and_the_rows_returned(
        self, google, download, clock: FakeClock, caplog: pytest.LogCaptureFixture
    ):
        google.serve(_QUERY, _RUN, _report("DONE"), (500, {"error": {"code": 500, "message": "Internal error"}}))

        with caplog.at_level(logging.WARNING, logger=api_module.__name__):
            rows = self._report(google)

        assert len(rows) == 2
        assert "Failed to delete query q" in caplog.text
