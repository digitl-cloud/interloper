"""Tests for the Impact Advertiser API engine.

Impact pages its lists by ``Page`` up to ``@numpages``, holds a page's records
under the resource's own name, refuses a request with an HTTP error status,
and exports a report or the clicks as a job that is queued, polled and
downloaded page by page. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.impact import constants
from interloper_assets.impact.api import ImpactApi

ACCOUNT = f"{constants.BASE_URL}/Advertisers/SID"
DOWNLOAD = f"{ACCOUNT}/Jobs/J1/Download"
DAY = dt.date(2026, 6, 10)
WINDOW = {"StartDate": "2026-06-10T00:00:00Z", "EndDate": "2026-06-10T23:59:59Z"}


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> ImpactApi:
    return ImpactApi(il.AsyncRESTClient(ACCOUNT, transport=httpx2.MockTransport(handler)))


def _sent(requests: list[httpx2.Request]) -> list[tuple[str, str, dict[str, str]]]:
    return [(request.method, str(request.url.copy_with(query=None)), dict(request.url.params)) for request in requests]


def _page(key: str, page: str) -> httpx2.Response:
    return httpx2.Response(200, json={"@page": page, "@numpages": "2", key: [{"Id": page}]})


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the engine's waits between polls instead of sleeping through them.

    Args:
        monkeypatch: Replaces the engine's ``sleep`` for the test only.

    Returns:
        The seconds of every wait, in order.
    """
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr("interloper_assets.impact.api.sleep", sleep)
    return waits


class _Job:
    """A fake Impact export: queues job ``J1``, answers its polls, serves its download.

    The download holds two pages of one record each under ``key``, unless
    ``pages`` says otherwise.
    """

    def __init__(self, key: str, statuses: list[str], pages: dict[str, httpx2.Response] | None = None) -> None:
        """Script the job.

        Args:
            key: The key each download page holds its records under.
            statuses: The status each poll answers, in order; the last repeats.
            pages: The response for each download page by its ``Page``; ``None`` for two pages of one record each.
        """
        self.key = key
        self.statuses = statuses
        self.pages = pages or {page: _page(key, page) for page in ("1", "2")}
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request and record it.

        Args:
            request: The request the engine sent.

        Returns:
            The queued job, the next poll's status or a download page.
        """
        self.requests.append(request)
        url = str(request.url.copy_with(query=None))
        if url == f"{ACCOUNT}/Jobs/J1":
            status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
            return httpx2.Response(200, json={"Id": "J1", "Status": status})
        if url == DOWNLOAD:
            return self.pages[request.url.params["Page"]]
        return httpx2.Response(200, json={"Status": "QUEUED", "QueuedUri": "/Advertisers/SID/Jobs/J1"})


class TestRows:
    async def test_every_page_is_read(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _page("Campaigns", request.url.params["Page"])

        assert await _api(handler).rows("/Campaigns", "Campaigns", Status="ACTIVE") == [{"Id": "1"}, {"Id": "2"}]
        assert _sent(requests) == [
            ("GET", f"{ACCOUNT}/Campaigns", {"Status": "ACTIVE", "Page": "1"}),
            ("GET", f"{ACCOUNT}/Campaigns", {"Status": "ACTIVE", "Page": "2"}),
        ]

    async def test_a_day_is_sent_as_impacts_window_over_it(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"@numpages": "1", "Actions": [{"Id": "A1"}]})

        assert await _api(handler).rows("/Actions", "Actions", DAY, CampaignId="123") == [{"Id": "A1"}]
        assert _sent(requests) == [("GET", f"{ACCOUNT}/Actions", {"CampaignId": "123", **WINDOW, "Page": "1"})]

    async def test_a_day_is_sent_as_the_window_an_endpoint_names(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"@numpages": "1", "Actions": [{"Id": "A1"}]})

        api = _api(handler)
        rows = await api.rows("/Actions", "Actions", DAY, window=("ActionDateStart", "ActionDateEnd"), CampaignId="123")

        assert rows == [{"Id": "A1"}]
        window = {"ActionDateStart": WINDOW["StartDate"], "ActionDateEnd": WINDOW["EndDate"]}
        assert _sent(requests) == [("GET", f"{ACCOUNT}/Actions", {"CampaignId": "123", **window, "Page": "1"})]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params["Page"] == "2":
                return httpx2.Response(403, json={"Status": "ERROR", "Message": "Forbidden"})
            return httpx2.Response(200, json={"@numpages": "2", "Actions": [{"Id": "1"}]})

        with pytest.raises(httpx2.HTTPStatusError, match="403 Forbidden"):
            await _api(handler).rows("/Actions", "Actions", DAY, CampaignId="123")

    async def test_an_http_error_raises(self):
        with pytest.raises(httpx2.HTTPStatusError, match="401 Unauthorized"):
            await _api(lambda _: httpx2.Response(401)).rows("/Campaigns", "Campaigns")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"@numpages": "0", "Actions": []})

        assert await _api(handler).rows("/Actions", "Actions", DAY, CampaignId="123") == []
        assert len(requests) == 1

    @pytest.mark.parametrize("page", [{"@numpages": "1", "Actions": None}, {"@numpages": "1"}])
    async def test_a_page_without_records_holds_none(self, page: dict[str, str | None]):
        assert await _api(lambda _: httpx2.Response(200, json=page)).rows("/Actions", "Actions") == []

    async def test_a_response_without_paging_info_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page ignores ``Page`` and answers the same list; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(400)
            return httpx2.Response(200, json={"Campaigns": [{"Id": "1"}]})

        assert await _api(handler).rows("/Campaigns", "Campaigns") == [{"Id": "1"}]
        assert len(requests) == 1


class TestExport:
    async def test_queues_the_export_waits_for_the_job_and_downloads_every_page(self, sleeps: list[float]):
        job = _Job("Clicks", ["COMPLETED"])

        rows = await _api(job).export("/Programs/123/ClickExport", "Clicks", Date="2026-06-10")

        assert rows == [{"Id": "1"}, {"Id": "2"}]
        assert _sent(job.requests) == [
            ("GET", f"{ACCOUNT}/Programs/123/ClickExport", {"Date": "2026-06-10", "ResultFormat": "JSON"}),
            ("GET", f"{ACCOUNT}/Jobs/J1", {}),
            ("GET", DOWNLOAD, {"Mode": "stream", "Page": "1"}),
            ("GET", DOWNLOAD, {"Mode": "stream", "Page": "2"}),
        ]
        assert [request.headers["Accept"] for request in job.requests[2:]] == ["application/octet-stream"] * 2
        assert sleeps == []

    async def test_polls_the_job_until_it_completes(self, sleeps: list[float], caplog: pytest.LogCaptureFixture):
        caplog.set_level("INFO", logger="interloper_assets.impact.api")
        job = _Job("Clicks", ["QUEUED", "IN_PROCESS", "COMPLETED"])

        assert await _api(job).export("/Programs/123/ClickExport", "Clicks") == [{"Id": "1"}, {"Id": "2"}]
        assert sleeps == [constants.JOB_POLL_INTERVAL] * 2
        assert caplog.messages == ["Waiting for Impact job J1..."] * 3

    @pytest.mark.parametrize(("status", "reason"), [(401, "Unauthorized"), (403, "Forbidden"), (503, "Unavailable")])
    async def test_a_refused_poll_raises_at_once(
        self, status: int, reason: str, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "JOB_POLL_TIMEOUT", 0.0)
        job = _Job("Clicks", ["COMPLETED"])

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("/Jobs/J1"):
                job.requests.append(request)
                return httpx2.Response(status)
            return job(request)

        with pytest.raises(httpx2.HTTPStatusError, match=f"{status} .*{reason}"):
            await _api(handler).export("/Programs/123/ClickExport", "Clicks")
        assert sleeps == []
        assert not any(str(request.url).startswith(DOWNLOAD) for request in job.requests)

    async def test_an_export_left_unqueued_raises_impacts_answer(self, sleeps: list[float]):
        answer = {"Status": "ERROR", "Message": "SUBAID is required"}

        with pytest.raises(RuntimeError, match=r"Impact did not queue /ReportExport/some_report: .*SUBAID is required"):
            await _api(lambda _: httpx2.Response(200, json=answer)).export("/ReportExport/some_report", "Records")
        assert sleeps == []

    async def test_a_poll_without_a_status_raises_at_once(self, sleeps: list[float]):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("/Jobs/J1"):
                return httpx2.Response(200, json={"Id": "J1"})
            return httpx2.Response(200, json={"QueuedUri": "/Advertisers/SID/Jobs/J1"})

        with pytest.raises(KeyError, match="Status"):
            await _api(handler).export("/Programs/123/ClickExport", "Clicks")
        assert sleeps == []

    @pytest.mark.parametrize("status", ["CANCELLED", "ERROR", "FAILED"])
    async def test_a_failed_job_raises_its_status(self, status: str, sleeps: list[float]):
        job = _Job("Records", [status])

        with pytest.raises(RuntimeError, match=f"Impact job J1 failed with status {status}"):
            await _api(job).export("/ReportExport/some_report", "Records")
        assert not any(str(request.url).startswith(DOWNLOAD) for request in job.requests)

    async def test_a_job_past_the_deadline_raises(self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]):
        monkeypatch.setattr(constants, "JOB_POLL_TIMEOUT", 0.0)
        job = _Job("Records", ["IN_PROCESS"])

        with pytest.raises(RuntimeError, match="Impact job J1 did not complete within 0s"):
            await _api(job).export("/ReportExport/some_report", "Records")
        assert sleeps == []

    async def test_a_refused_export_raises_without_polling(self, sleeps: list[float]):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(400, json={"Status": "ERROR", "Message": "Invalid report"})

        with pytest.raises(httpx2.HTTPStatusError, match="400 Bad Request"):
            await _api(handler).export("/ReportExport/some_report", "Records")
        assert len(requests) == 1

    async def test_a_refusal_on_a_later_download_page_raises(self, sleeps: list[float]):
        job = _Job("Records", ["COMPLETED"], pages={"1": _page("Records", "1"), "2": httpx2.Response(403)})

        with pytest.raises(httpx2.HTTPStatusError, match="403 Forbidden"):
            await _api(job).export("/ReportExport/some_report", "Records")

    async def test_an_empty_download_holds_no_rows(self, sleeps: list[float]):
        job = _Job("Records", ["COMPLETED"], pages={"1": httpx2.Response(200, json={"@numpages": "0", "Records": []})})

        assert await _api(job).export("/ReportExport/some_report", "Records") == []

    async def test_a_download_without_paging_info_is_read_once(self, sleeps: list[float]):
        job = _Job("Records", ["COMPLETED"], pages={"1": httpx2.Response(200, json={"Records": [{"Id": "1"}]})})

        assert await _api(job).export("/ReportExport/some_report", "Records") == [{"Id": "1"}]
        assert [str(request.url).startswith(DOWNLOAD) for request in job.requests].count(True) == 1


class TestReport:
    async def test_exports_one_day_of_a_report(self, sleeps: list[float]):
        job = _Job("Records", ["COMPLETED"])

        rows = await _api(job).report("att_adv_performance_by_day_pm_only", DAY, SUBAID="123")

        assert rows == [{"Id": "1"}, {"Id": "2"}]
        assert _sent(job.requests)[0] == (
            "GET",
            f"{ACCOUNT}/ReportExport/att_adv_performance_by_day_pm_only",
            {"SUBAID": "123", **WINDOW, "ResultFormat": "JSON"},
        )
