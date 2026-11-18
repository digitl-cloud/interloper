"""Tests for the Amazon Selling Partner API engine.

Reports and Data Kiosk queries are both jobs: created by a POST, polled until
``DONE``, ``CANCELLED`` or ``FATAL``, then read from a document at a signed URL
outside the API (gzipped when Amazon says a report's is, plain JSON for a
query). A poll that fails transiently is polled again, once a minute, until
the job's deadline; one refused for good raises at once. These tests pin each
step against a mock transport and a fake clock.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
from collections.abc import Generator
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.amazon_selling_partner import api as api_module
from interloper_assets.amazon_selling_partner.api import AmazonSellingPartnerApi

API_URL = "https://sellingpartnerapi-eu.amazon.com"
REPORTS = f"{API_URL}/reports/2021-06-30"
DATA_KIOSK = f"{API_URL}/dataKiosk/2023-11-15"
DOCUMENT_URL = "https://documents.example.com/document"
TRAFFIC: dict[str, Any] = {"reportType": "GET_VENDOR_TRAFFIC_REPORT", "marketplaceIds": ["A1PA6795UKMFR9"]}


class FakeJob:
    """Amazon's side of one job, recording every request.

    ``polls`` are the job's answers to successive polls, each a status, an
    HTTP error status or a transport error to raise; the last repeats.
    ``failed_downloads`` answer the first downloads of the document the same
    way. ``responses`` overrides the answer to a ``(method, url)``.
    """

    def __init__(self) -> None:
        """Serve a job that finishes at its first poll, with a document and no content yet."""
        self.document: Any = None
        self.compressed = True
        self.polls: list[str | int | httpx2.TransportError] = ["DONE"]
        self.failed_downloads: list[int | httpx2.TransportError] = []
        self.has_document = True
        self.responses: dict[tuple[str, str], httpx2.Response] = {}
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer a request as Amazon would, at the API or at the document's signed URL.

        Args:
            request: The request sent.

        Returns:
            The job's answer.
        """
        self.requests.append(request)
        call = (request.method, str(request.url))
        if call in self.responses:
            return self.responses[call]
        if call == ("GET", DOCUMENT_URL):
            return self._download()
        if call in {("GET", f"{REPORTS}/reports/R1"), ("GET", f"{DATA_KIOSK}/queries/Q1")}:
            return self._poll()
        report_document = {"reportDocumentId": "D1", "url": DOCUMENT_URL}
        if self.compressed:
            report_document["compressionAlgorithm"] = "GZIP"
        answers = {
            ("POST", f"{REPORTS}/reports"): {"reportId": "R1"},
            ("GET", f"{REPORTS}/documents/D1"): report_document,
            ("POST", f"{DATA_KIOSK}/queries"): {"queryId": "Q1"},
            ("GET", f"{DATA_KIOSK}/documents/D1"): {"documentId": "D1", "documentUrl": DOCUMENT_URL},
        }
        return httpx2.Response(200, json=answers[call])

    @property
    def reports(self) -> bool:
        """Whether the job is a report.

        Returns:
            True for a report, whose document is gzipped; False for a query.
        """
        return str(self.requests[0].url).startswith(REPORTS)

    @property
    def calls(self) -> list[tuple[str, str]]:
        """List the requests the job received.

        Returns:
            Each request's method and URL, in the order they were sent.
        """
        return [(request.method, str(request.url)) for request in self.requests]

    def _download(self) -> httpx2.Response:
        """Answer a download of the document, gzipped when a report's document is compressed.

        A failed download that is a transport error is raised, as a dropped connection would be.

        Returns:
            The document, or the next failed download's refusal.
        """
        if self.failed_downloads:
            failure = self.failed_downloads.pop(0)
            if isinstance(failure, httpx2.TransportError):
                raise failure
            return httpx2.Response(failure)
        content = json.dumps(self.document).encode()
        return httpx2.Response(200, content=gzip.compress(content) if self.reports and self.compressed else content)

    def _poll(self) -> httpx2.Response:
        """Answer the next poll, raising it when the poll's answer is a transport error.

        Returns:
            The job's status, or a refusal.
        """
        polled = sum(1 for request in self.requests if request.method == "GET" and "/D1" not in request.url.path)
        answer = self.polls[min(polled, len(self.polls)) - 1]
        if isinstance(answer, httpx2.TransportError):
            raise answer
        if isinstance(answer, int):
            return httpx2.Response(answer, json={"errors": [{"code": "Unauthorized"}]})
        job: dict[str, Any] = {"processingStatus": answer}
        if self.has_document:
            job["reportDocumentId" if self.reports else "dataDocumentId"] = "D1"
        return httpx2.Response(200, json=job)


class Clock:
    """The engine's event-loop clock, advanced only by the engine's waits."""

    def __init__(self) -> None:
        """Start the clock at zero, with no wait recorded."""
        self.now = 0.0
        self.waits: list[float] = []

    def time(self) -> float:
        """Read the clock.

        Returns:
            The seconds waited so far.
        """
        return self.now

    async def sleep(self, seconds: float) -> None:
        """Record a wait and advance the clock past it, without waiting.

        Args:
            seconds: The wait's length.
        """
        self.waits.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    """Run the engine's polling on a fake clock.

    Args:
        monkeypatch: Replaces the engine's event loop clock and ``sleep`` for the test only.

    Returns:
        The clock, recording every wait.
    """
    fake = Clock()
    monkeypatch.setattr(api_module, "get_running_loop", lambda: fake)
    monkeypatch.setattr(api_module, "sleep", fake.sleep)
    return fake


@pytest.fixture
def vendor(clock: Clock) -> FakeJob:
    """Serve the API and the signed documents from one fake, polling on the fake clock.

    Args:
        clock: The fake clock the engine polls on.

    Returns:
        The fake, its document and polls left to each test.
    """
    return FakeJob()


class AccessToken(httpx2.Auth):
    """Login with Amazon's access token, carried in ``x-amz-access-token`` as the connection's auth does."""

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        """Carry the token.

        Args:
            request: The request to authenticate.

        Yields:
            The authenticated request.
        """
        request.headers["x-amz-access-token"] = "token"
        yield request


def _api(vendor: FakeJob, *, attempts: int = 1) -> AmazonSellingPartnerApi:
    """Build the engine on a client served by the fake, repeating no request itself unless told to.

    Args:
        vendor: The fake answering every request.
        attempts: How many times the client sends a request that fails transiently, without waiting.

    Returns:
        The engine.
    """
    client = il.AsyncRESTClient(
        API_URL,
        auth=AccessToken(),
        transport=httpx2.MockTransport(vendor),
        timeout=60,
        retry=il.RetryPolicy(max_attempts=attempts, delay=0),
    )
    return AmazonSellingPartnerApi(client)


REPORT_CALLS = [
    ("POST", f"{REPORTS}/reports"),
    ("GET", f"{REPORTS}/reports/R1"),
    ("GET", f"{REPORTS}/documents/D1"),
    ("GET", DOCUMENT_URL),
]
QUERY_CALLS = [
    ("POST", f"{DATA_KIOSK}/queries"),
    ("GET", f"{DATA_KIOSK}/queries/Q1"),
    ("GET", f"{DATA_KIOSK}/documents/D1"),
    ("GET", DOCUMENT_URL),
]


class TestReport:
    async def test_runs_a_job_over_whole_days_and_reads_the_named_list(self, vendor: FakeJob):
        rows = [{"asin": "B1"}, {"asin": "B2"}]
        vendor.document = {"reportSpecification": {}, "trafficByAsin": rows, "trafficAggregate": [{"x": 1}]}

        result = await _api(vendor).report(
            key="trafficByAsin",
            start_date=dt.date(2026, 10, 1),
            end_date=dt.date(2026, 10, 3),
            **TRAFFIC,
            reportOptions={"reportPeriod": "DAY"},
        )

        assert result == rows
        assert vendor.calls == REPORT_CALLS
        body = json.loads(vendor.requests[0].content)
        assert body == {
            **TRAFFIC,
            "reportOptions": {"reportPeriod": "DAY"},
            "dataStartTime": "2026-10-01T00:00:00.000Z",
            "dataEndTime": "2026-10-03T23:59:59.999Z",
        }
        assert list(body) == ["reportType", "marketplaceIds", "reportOptions", "dataStartTime", "dataEndTime"]

    @pytest.mark.parametrize(
        ("window", "sent"),
        [
            ({}, {}),
            ({"start_date": dt.date(2026, 10, 1)}, {"dataStartTime": "2026-10-01T00:00:00.000Z"}),
            ({"end_date": dt.date(2026, 10, 1)}, {"dataEndTime": "2026-10-01T23:59:59.999Z"}),
        ],
    )
    async def test_each_end_of_the_window_is_optional(
        self, vendor: FakeJob, window: dict[str, dt.date], sent: dict[str, str]
    ):
        vendor.document = {"forecastByAsin": []}

        await _api(vendor).report(key="forecastByAsin", **window, **TRAFFIC)

        assert json.loads(vendor.requests[0].content) == {**TRAFFIC, **sent}

    async def test_a_window_cannot_also_be_given_in_amazons_names(self, vendor: FakeJob):
        with pytest.raises(TypeError, match="dataStartTime"):
            await _api(vendor).report(
                key="trafficByAsin", start_date=dt.date(2026, 10, 1), dataStartTime="2026-10-01T00:00:00Z", **TRAFFIC
            )
        assert vendor.requests == []

    async def test_the_document_is_downloaded_without_the_api_credentials(self, vendor: FakeJob):
        vendor.document = {"trafficByAsin": []}

        await _api(vendor).report(key="trafficByAsin", **TRAFFIC)

        api_request, download = vendor.requests[0], vendor.requests[-1]
        assert api_request.headers["x-amz-access-token"] == "token"
        assert "x-amz-access-token" not in download.headers
        assert download.extensions["timeout"] == {"connect": 60, "read": 60, "write": 60, "pool": 60}

    async def test_an_uncompressed_document_is_read_as_is(self, vendor: FakeJob):
        vendor.document = {"trafficByAsin": [{"asin": "B1"}]}
        vendor.compressed = False

        assert await _api(vendor).report(key="trafficByAsin", **TRAFFIC) == [{"asin": "B1"}]

    @pytest.mark.parametrize("failure", [503, 429, httpx2.ReadTimeout("timed out")], ids=str)
    async def test_a_download_failing_transiently_is_downloaded_again(
        self, vendor: FakeJob, failure: int | httpx2.TransportError
    ):
        vendor.document = {"trafficByAsin": [{"asin": "B1"}]}
        vendor.failed_downloads = [failure]

        assert await _api(vendor, attempts=2).report(key="trafficByAsin", **TRAFFIC) == [{"asin": "B1"}]
        assert vendor.calls.count(("GET", DOCUMENT_URL)) == 2

    async def test_a_download_failing_past_its_attempts_raises(self, vendor: FakeJob):
        vendor.failed_downloads = [503, 503]

        with pytest.raises(httpx2.HTTPStatusError, match="503"):
            await _api(vendor, attempts=2).report(key="trafficByAsin", **TRAFFIC)

    @pytest.mark.parametrize("document", [{"reportSpecification": {}}, {}, [], None])
    async def test_a_document_without_the_list_reads_no_rows(self, vendor: FakeJob, document: Any):
        vendor.document = document

        assert await _api(vendor).report(key="trafficByAsin", **TRAFFIC) == []

    async def test_a_finished_job_is_read_without_waiting(self, vendor: FakeJob, clock: Clock):
        vendor.document = {"trafficByAsin": []}

        await _api(vendor).report(key="trafficByAsin", **TRAFFIC)

        assert clock.waits == []

    async def test_a_job_in_progress_is_polled_again_a_minute_later(self, vendor: FakeJob, clock: Clock):
        vendor.document = {"trafficByAsin": [{"asin": "B1"}]}
        vendor.polls = ["IN_QUEUE", "IN_PROGRESS", "DONE"]

        assert await _api(vendor).report(key="trafficByAsin", **TRAFFIC) == [{"asin": "B1"}]
        assert vendor.calls == REPORT_CALLS[:2] + REPORT_CALLS[1:2] * 2 + REPORT_CALLS[2:]
        assert clock.waits == [60.0, 60.0]

    @pytest.mark.parametrize("failure", [429, 503, httpx2.ConnectError("connection refused")], ids=str)
    async def test_a_poll_failing_transiently_is_polled_again(
        self, vendor: FakeJob, clock: Clock, failure: int | httpx2.TransportError
    ):
        vendor.document = {"trafficByAsin": [{"asin": "B1"}]}
        vendor.polls = ["IN_PROGRESS", failure, "DONE"]

        assert await _api(vendor).report(key="trafficByAsin", **TRAFFIC) == [{"asin": "B1"}]
        assert vendor.calls.count(("GET", f"{REPORTS}/reports/R1")) == 3
        assert clock.waits == [60.0, 60.0]

    async def test_a_job_unfinished_after_two_hours_raises(self, vendor: FakeJob, clock: Clock):
        vendor.polls = ["IN_PROGRESS"]

        with pytest.raises(RuntimeError, match=r"^Report R1 did not complete within 7200s$"):
            await _api(vendor).report(key="trafficByAsin", **TRAFFIC)
        assert clock.waits == [60.0] * 120

    @pytest.mark.parametrize("status", [400, 403, 404])
    async def test_a_poll_refused_for_good_raises_at_once(self, vendor: FakeJob, clock: Clock, status: int):
        vendor.polls = ["IN_PROGRESS", status]

        with pytest.raises(httpx2.HTTPStatusError, match=str(status)):
            await _api(vendor).report(key="trafficByAsin", **TRAFFIC)
        assert vendor.calls == REPORT_CALLS[:2] + REPORT_CALLS[1:2]
        assert clock.waits == [60.0]

    async def test_a_poll_failing_past_the_deadline_raises_with_the_failure(self, vendor: FakeJob, clock: Clock):
        vendor.polls = [503]

        with pytest.raises(RuntimeError, match=r"^Report R1 did not complete within 7200s$") as raised:
            await _api(vendor).report(key="trafficByAsin", **TRAFFIC)
        assert isinstance(raised.value.__cause__, httpx2.HTTPStatusError)
        assert clock.now == 7200

    @pytest.mark.parametrize("compressed", [True, False])
    async def test_a_day_amazon_has_no_data_for_yet_reads_no_rows(self, vendor: FakeJob, compressed: bool):
        vendor.document = {"errorDetails": "Data for the requested range is not yet available"}
        vendor.compressed = compressed
        vendor.polls = ["FATAL"]

        assert await _api(vendor).report(key="trafficByAsin", **TRAFFIC) == []
        assert vendor.calls == REPORT_CALLS

    @pytest.mark.parametrize(
        ("document", "message"),
        [
            ({"errorDetails": "The report type is not allowed"}, "Report R1 failed: The report type is not allowed"),
            ({}, "Report R1 failed: CANCELLED"),
            (["not", "an", "object"], "Report R1 failed: CANCELLED"),
            ({"errorDetails": 42}, "Report R1 failed: 42"),
            ({"errorDetails": {"code": "x"}}, "Report R1 failed: {'code': 'x'}"),
        ],
    )
    async def test_a_failed_job_raises_amazons_details(self, vendor: FakeJob, document: Any, message: str):
        vendor.document = document
        vendor.polls = ["CANCELLED"]

        with pytest.raises(RuntimeError, match=f"^{message}$"):
            await _api(vendor).report(key="trafficByAsin", **TRAFFIC)

    @pytest.mark.parametrize("status", ["DONE", "CANCELLED", "FATAL"])
    async def test_a_job_without_a_document_raises_its_status(self, vendor: FakeJob, status: str):
        vendor.polls = [status]
        vendor.has_document = False

        with pytest.raises(RuntimeError, match=f"^Report R1 failed with status {status}$"):
            await _api(vendor).report(key="trafficByAsin", **TRAFFIC)
        assert vendor.calls == REPORT_CALLS[:2]

    @pytest.mark.parametrize("refused", REPORT_CALLS[:1] + REPORT_CALLS[2:])
    async def test_a_refusal_outside_polling_raises(self, vendor: FakeJob, refused: tuple[str, str]):
        vendor.document = {"trafficByAsin": []}
        vendor.responses[refused] = httpx2.Response(400, json={"errors": [{"code": "InvalidInput"}]})

        with pytest.raises(httpx2.HTTPStatusError):
            await _api(vendor).report(key="trafficByAsin", **TRAFFIC)
        assert vendor.calls == REPORT_CALLS[: REPORT_CALLS.index(refused) + 1]


class TestQuery:
    async def test_runs_a_query_and_reads_the_named_list(self, vendor: FakeJob):
        rows = [{"groupByKey": {"asin": "B1"}}]
        vendor.document = {"metrics": rows}

        assert await _api(vendor).query(key="metrics", query="{ sourcingView }") == rows
        assert vendor.calls == QUERY_CALLS
        assert json.loads(vendor.requests[0].content) == {"query": "{ sourcingView }"}

    async def test_the_list_is_found_nested_in_the_queried_view(self, vendor: FakeJob):
        rows = [{"groupByKey": {"asin": "B1"}}]
        vendor.document = {
            "schema": {"other": {"metrics": "not a list"}, "empty": {"metrics": []}, "view": {"metrics": rows}}
        }

        assert await _api(vendor).query(key="metrics", query="{ sourcingView }") == rows

    @pytest.mark.parametrize("document", [{"errors": []}, ["not", "an", "object"], {"metrics": None}])
    async def test_a_document_without_the_list_reads_no_rows(self, vendor: FakeJob, document: Any):
        vendor.document = document

        assert await _api(vendor).query(key="metrics", query="{ sourcingView }") == []

    async def test_the_document_is_plain_json_downloaded_without_the_api_credentials(self, vendor: FakeJob):
        vendor.document = {"metrics": []}

        await _api(vendor).query(key="metrics", query="{ sourcingView }")

        download = vendor.requests[-1]
        assert "x-amz-access-token" not in download.headers
        assert download.extensions["timeout"] == {"connect": 60, "read": 60, "write": 60, "pool": 60}

    async def test_a_query_without_a_document_reads_no_rows(self, vendor: FakeJob):
        vendor.has_document = False

        assert await _api(vendor).query(key="metrics", query="{ sourcingView }") == []
        assert vendor.calls == QUERY_CALLS[:2]

    async def test_a_query_in_progress_or_failing_transiently_is_polled_again_a_minute_later(
        self, vendor: FakeJob, clock: Clock
    ):
        vendor.document = {"metrics": [{"asin": "B1"}]}
        vendor.polls = ["IN_QUEUE", 503, "DONE"]

        assert await _api(vendor).query(key="metrics", query="{ sourcingView }") == [{"asin": "B1"}]
        assert vendor.calls.count(("GET", f"{DATA_KIOSK}/queries/Q1")) == 3
        assert clock.waits == [60.0, 60.0]

    async def test_a_query_poll_refused_for_good_raises_at_once(self, vendor: FakeJob, clock: Clock):
        vendor.polls = [403]

        with pytest.raises(httpx2.HTTPStatusError, match="403"):
            await _api(vendor).query(key="metrics", query="{ sourcingView }")
        assert clock.waits == []

    @pytest.mark.parametrize("status", ["CANCELLED", "FATAL"])
    async def test_a_failed_query_raises_its_status(self, vendor: FakeJob, status: str):
        vendor.polls = [status]

        with pytest.raises(RuntimeError, match=f"^Query Q1 failed with status {status}$"):
            await _api(vendor).query(key="metrics", query="{ sourcingView }")
        assert vendor.calls == QUERY_CALLS[:2]

    async def test_a_query_unfinished_after_an_hour_raises(self, vendor: FakeJob, clock: Clock):
        vendor.polls = ["IN_PROGRESS"]

        with pytest.raises(RuntimeError, match=r"^Query Q1 did not complete within 3600s$"):
            await _api(vendor).query(key="metrics", query="{ sourcingView }")
        assert clock.waits == [60.0] * 60

    @pytest.mark.parametrize("refused", QUERY_CALLS[:1] + QUERY_CALLS[2:])
    async def test_a_refusal_outside_polling_raises(self, vendor: FakeJob, refused: tuple[str, str]):
        vendor.document = {"metrics": []}
        vendor.responses[refused] = httpx2.Response(400, json={"errors": [{"code": "InvalidInput"}]})

        with pytest.raises(httpx2.HTTPStatusError):
            await _api(vendor).query(key="metrics", query="{ sourcingView }")
        assert vendor.calls == QUERY_CALLS[: QUERY_CALLS.index(refused) + 1]
