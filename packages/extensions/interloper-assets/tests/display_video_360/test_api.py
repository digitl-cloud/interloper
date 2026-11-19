"""Tests for the Display & Video 360 API engine.

Entities are read from the Display & Video API, a list paged by
``nextPageToken``; reports are Bid Manager queries, created, run, polled with
a capped backoff, downloaded as CSV with a summary footer and deleted. These
tests pin every request at the HTTP level through the ``google`` fake.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import httpx2
import pandas as pd
import pytest

from interloper_assets.display_video_360 import api as api_module
from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.api import DisplayVideo360Api

_DISPLAY_VIDEO = "https://displayvideo.googleapis.com/v4"
_BID_MANAGER = "https://doubleclickbidmanager.googleapis.com/v2"
_REFUSAL = {"error": {"code": 403, "message": "The caller does not have permission"}}


def _api(google: Any) -> DisplayVideo360Api:
    return DisplayVideo360Api(google.client)


class TestGet:
    async def test_reads_one_object(self, google):
        google.serve({"name": "audience"})

        audience = await _api(google).get("/firstPartyAndPartnerAudiences/7", partnerId="1")

        assert audience == {"name": "audience"}
        assert google.requests == [("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences/7?partnerId=1", None)]

    async def test_a_refusal_raises_its_status(self, google):
        google.serve((403, _REFUSAL))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await _api(google).get("/partners/1")

        assert raised.value.response.status_code == 403


class TestRows:
    async def test_pages_until_no_next_page_token(self, google):
        google.serve(
            {"firstPartyAndPartnerAudiences": [{"name": "a"}], "nextPageToken": "next"},
            {"firstPartyAndPartnerAudiences": [{"name": "b"}]},
        )

        rows = await _api(google).rows("/firstPartyAndPartnerAudiences", partnerId="1")

        assert rows == [{"name": "a"}, {"name": "b"}]
        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences?partnerId=1", None),
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences?partnerId=1&pageToken=next", None),
        ]

    async def test_a_page_without_rows_still_pages_on(self, google):
        google.serve({"nextPageToken": "next"}, {"partners": [{"partnerId": "2"}]})

        assert await _api(google).rows("/partners") == [{"partnerId": "2"}]

    async def test_rows_are_read_from_the_pages_one_list(self, google):
        google.serve({"customBiddingScripts": [{"customBiddingScriptId": "4"}]})

        rows = await _api(google).rows("/customBiddingAlgorithms/5/scripts", advertiserId="9")

        assert rows == [{"customBiddingScriptId": "4"}]
        assert google.requests == [("GET", f"{_DISPLAY_VIDEO}/customBiddingAlgorithms/5/scripts?advertiserId=9", None)]

    async def test_a_page_of_two_lists_is_refused(self, google):
        google.serve({"partners": [{"partnerId": "1"}], "advertisers": [], "nextPageToken": "next"})

        with pytest.raises(
            RuntimeError,
            match=(
                r"^A page of /partners carries more than one field beside nextPageToken: "
                r"\['partners', 'advertisers'\]$"
            ),
        ):
            await _api(google).rows("/partners")

    async def test_an_empty_list_is_no_rows(self, google):
        assert await _api(google).rows("/partners") == []

    async def test_a_refusal_raises_its_status(self, google):
        google.serve((401, {"error": {"code": 401, "message": "Request had invalid authentication credentials"}}))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await _api(google).rows("/partners")

        assert raised.value.response.status_code == 401


class FakeClock:
    """Stand in for the engine's clock: sleeping moves it on instead of waiting."""

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

    async def sleep(self, seconds: float) -> None:
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
_POLL = ("GET", f"{_BID_MANAGER}/queries/q/reports/r", None)
_DELETE = ("DELETE", f"{_BID_MANAGER}/queries/q", None)


class TestReport:
    @pytest.fixture
    def clock(self, monkeypatch: pytest.MonkeyPatch) -> FakeClock:
        """Give the engine a clock of its own, which sleeps without waiting.

        Args:
            monkeypatch: The pytest monkeypatch fixture that restores the engine's clock after the test.

        Returns:
            The clock, holding the waits the engine asked for, in seconds.
        """
        fake = FakeClock()
        monkeypatch.setattr(api_module, "monotonic", fake.monotonic)
        monkeypatch.setattr(api_module, "sleep", fake.sleep)
        return fake

    async def _report(self, google: Any, **params: Any) -> list[dict[str, Any]]:
        return await _api(google).report(dt.date(2026, 7, 10), title="line_items_stats_2026-07-10", **params)

    async def test_runs_one_query_over_its_day_and_deletes_it(self, google, download, clock: FakeClock):
        google.serve(_QUERY, _RUN, _report("DONE"))
        params: dict[str, Any] = {
            "type": "STANDARD",
            "groupBys": ["FILTER_DATE"],
            "metrics": ["METRIC_IMPRESSIONS"],
            "filters": [{"type": "FILTER_PARTNER", "value": "1"}],
        }

        rows = await self._report(google, **params)

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
            ("POST", f"{_BID_MANAGER}/queries", query),
            ("POST", f"{_BID_MANAGER}/queries/q:run?synchronous=false", None),
            _POLL,
            _DELETE,
        ]
        assert clock.sleeps == []
        assert len(rows) == 2

    async def test_the_file_is_downloaded_without_the_token_or_a_timeout(self, google, download, clock: FakeClock):
        google.serve(_QUERY, _RUN, _report("DONE"))

        await self._report(google)

        assert download.downloads == [{"url": "https://storage/report.csv", "authorization": None, "timeout": None}]

    async def test_the_csv_footer_is_dropped_and_a_dash_is_missing(self, google, download, clock: FakeClock):
        download.text = "Date,Advertiser ID,Impressions,Revenue\n2026/07/10,9,100,-\n2026/07/10,9,200,1.5\n,,300,1.5\n"
        google.serve(_QUERY, _RUN, _report("DONE"))

        rows = await self._report(google)

        assert rows[1] == {"Date": "2026/07/10", "Advertiser ID": "9", "Impressions": "200", "Revenue": "1.5"}
        assert rows[0]["Impressions"] == "100"
        assert pd.isna(rows[0]["Revenue"])

    async def test_an_id_beside_a_dash_keeps_its_digits(self, google, download, clock: FakeClock):
        download.text = (
            "Date,Line Item ID,CM360 Placement ID,Impressions\n"
            "2026/07/10,9007199254740993,-,100\n"
            "2026/07/10,8,123,200\n"
            ",,,300\n"
        )
        google.serve(_QUERY, _RUN, _report("DONE"))

        rows = await self._report(google)

        assert [row["Line Item ID"] for row in rows] == ["9007199254740993", "8"]
        assert rows[1]["CM360 Placement ID"] == "123"
        assert pd.isna(rows[0]["CM360 Placement ID"])

    @pytest.mark.parametrize(
        "text",
        ["Date,Impressions\nNo data returned by the reporting service.,\n", "Date,Impressions\n"],
        ids=["no-data-row", "headers-only"],
    )
    async def test_a_report_without_data_is_no_rows(self, google, download, clock: FakeClock, text: str):
        download.text = text
        google.serve(_QUERY, _RUN, _report("DONE"))

        assert await self._report(google) == []

    async def test_polls_with_a_doubling_wait_capped_at_the_maximum(self, google, download, clock: FakeClock):
        google.serve(_QUERY, _RUN, *[_report("RUNNING")] * 7, _report("DONE"))

        rows = await self._report(google)

        assert clock.sleeps == [60, 120, 240, 480, 960, constants.MAX_RETRY_INTERVAL, constants.MAX_RETRY_INTERVAL]
        assert google.requests.count(_POLL) == 8
        assert len(rows) == 2

    async def test_a_failed_report_raises_and_still_deletes_the_query(self, google, clock: FakeClock):
        google.serve(_QUERY, _RUN, _report("FAILED"))

        with pytest.raises(RuntimeError, match=r"^Report r \(query q\) failed$"):
            await self._report(google)

        assert google.requests[-1] == _DELETE

    async def test_a_report_never_done_times_out_and_still_deletes_the_query(self, google, clock: FakeClock):
        google.serve(_QUERY, _RUN, *[_report("RUNNING")] * 14)

        with pytest.raises(RuntimeError, match=r"^Report r did not complete within 10800s$"):
            await self._report(google)

        assert clock.sleeps == [60, 120, 240, 480, 960] + [constants.MAX_RETRY_INTERVAL] * 8
        assert clock.now >= constants.REPORT_TIMEOUT
        assert google.requests[-1] == _DELETE

    async def test_a_refused_run_raises_its_status_and_deletes_the_query(self, google, clock: FakeClock):
        google.serve(_QUERY, (403, _REFUSAL))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await self._report(google)

        assert raised.value.response.status_code == 403
        assert google.requests[-1] == _DELETE

    async def test_a_failed_download_raises_and_deletes_the_query(self, google, download, clock: FakeClock):
        download.status = 403
        google.serve(_QUERY, _RUN, _report("DONE"))

        with pytest.raises(httpx2.HTTPStatusError):
            await self._report(google)

        assert google.requests[-1] == _DELETE

    async def test_a_failed_delete_is_logged_and_the_rows_returned(
        self, google, download, clock: FakeClock, caplog: pytest.LogCaptureFixture
    ):
        google.serve(_QUERY, _RUN, _report("DONE"), (500, {"error": {"code": 500, "message": "Internal error"}}))

        with caplog.at_level(logging.WARNING, logger=api_module.__name__):
            rows = await self._report(google)

        assert len(rows) == 2
        assert "Failed to delete query q" in caplog.text
