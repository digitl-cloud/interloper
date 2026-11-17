"""Tests for the Teads API engine.

Teads runs a report as a job: requested with the report's filters, dimensions
and metrics, polled until it finishes, then downloaded as a CSV from a URI that
may point off the API's host. It refuses a report it cannot run with
``valid: false`` and any other request with an HTTP error status. These tests
pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.teads import constants
from interloper_assets.teads.api import TeadsApi

DAY = dt.date(2026, 6, 14)
UTC = "Etc/GMT+0"
FILE = "https://storage.example/r-1.csv"
CSV = "Day,Creative ID,Clicks\n2026-06-14,222,42\n2026-06-14,223,\n"
WINDOW = {
    "start": "2026-06-14T00:00:00.000+00:00",
    "end": "2026-06-14T23:59:59.999+00:00",
    "timezone": "Etc/GMT+0",
}


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> TeadsApi:
    return TeadsApi(il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler)))


def _sent(requests: list[httpx2.Request]) -> list[tuple[str, str]]:
    return [(request.method, str(request.url)) for request in requests]


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

    monkeypatch.setattr("interloper_assets.teads.api.sleep", sleep)
    return waits


class _Storage:
    """A fake file host serving the report's CSV, recording what the engine asked of it."""

    def __init__(self, files: dict[str, httpx2.Response] | None = None) -> None:
        """Script the host.

        Args:
            files: The response for each URL; ``None`` serves ``CSV`` at ``FILE``.
        """
        self.files = files or {FILE: httpx2.Response(200, text=CSV)}
        self.requests: list[httpx2.Request] = []
        self.timeouts: list[Any] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Make every plain ``httpx2.AsyncClient`` the engine opens answer from this host.

        Args:
            monkeypatch: Replaces ``httpx2.AsyncClient`` for the test only.
        """
        client = httpx2.AsyncClient

        def open_client(**kwargs: Any) -> httpx2.AsyncClient:
            self.timeouts.append(kwargs.get("timeout"))
            return client(transport=httpx2.MockTransport(self), **kwargs)

        monkeypatch.setattr(httpx2, "AsyncClient", open_client)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one download and record it.

        Args:
            request: The request the engine sent.

        Returns:
            The file at the request's URL.
        """
        self.requests.append(request)
        return self.files[str(request.url)]


class _Teads:
    """A fake Teads API: runs report ``r-1`` and answers its polls."""

    def __init__(self, statuses: list[dict[str, Any]], run: httpx2.Response | None = None) -> None:
        """Script the report.

        Args:
            statuses: The status each poll answers, in order.
            run: The answer to the report request; ``None`` for a valid job ``r-1``.
        """
        self.statuses = list(statuses)
        self.run = run or httpx2.Response(200, json={"valid": True, "id": "r-1"})
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request and record it.

        Args:
            request: The request the engine sent.

        Returns:
            The report job, or the next poll's status.
        """
        self.requests.append(request)
        if request.url.path == "/api/reports/run":
            return self.run
        return httpx2.Response(200, json=self.statuses.pop(0))


def _finished(uri: str = FILE) -> dict[str, Any]:
    return {"status": "finished", "uri": uri}


class TestReport:
    async def test_runs_the_report_over_the_day_and_returns_its_csv_rows_as_written(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        teads = _Teads([_finished()])
        storage = _Storage()
        storage.install(monkeypatch)

        rows = await _api(teads).report(DAY, UTC, {"advertisers": [20765]}, dimensions=["day"], metrics=["click"])

        assert rows == [
            {"Day": "2026-06-14", "Creative ID": "222", "Clicks": "42"},
            {"Day": "2026-06-14", "Creative ID": "223", "Clicks": ""},
        ]
        assert _sent(teads.requests) == [
            ("POST", f"{constants.BASE_URL}/api/reports/run"),
            ("GET", f"{constants.BASE_URL}/api/reports/status/r-1"),
        ]
        body = json.loads(teads.requests[0].content)
        assert body == {
            "filters": {"date": WINDOW, "advertisers": [20765]},
            "dimensions": ["day"],
            "metrics": ["click"],
        }
        assert list(body) == ["filters", "dimensions", "metrics"]
        assert list(body["filters"]) == ["date", "advertisers"]
        assert list(body["filters"]["date"]) == ["start", "end", "timezone"]
        assert _sent(storage.requests) == [("GET", FILE)]
        assert sleeps == []

    async def test_the_report_is_whatever_the_caller_asks_beside_its_day(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        teads = _Teads([_finished()])
        _Storage().install(monkeypatch)

        await _api(teads).report(DAY, UTC, {}, dimensions=["creative_id"], metrics=["start"], extra={"a": 1})

        assert json.loads(teads.requests[0].content) == {
            "filters": {"date": WINDOW},
            "dimensions": ["creative_id"],
            "metrics": ["start"],
            "extra": {"a": 1},
        }

    @pytest.mark.parametrize(
        ("day", "timezone", "start", "end"),
        [
            (DAY, "Europe/Paris", "2026-06-14T00:00:00.000+02:00", "2026-06-14T23:59:59.999+02:00"),
            (dt.date(2026, 3, 29), "Europe/Paris", "2026-03-29T00:00:00.000+01:00", "2026-03-29T23:59:59.999+02:00"),
            (DAY, "America/New_York", "2026-06-14T00:00:00.000-04:00", "2026-06-14T23:59:59.999-04:00"),
        ],
    )
    async def test_the_day_is_read_in_the_callers_timezone(
        self, day: dt.date, timezone: str, start: str, end: str, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        teads = _Teads([_finished()])
        _Storage().install(monkeypatch)

        await _api(teads).report(day, timezone, {})

        date = json.loads(teads.requests[0].content)["filters"]["date"]
        assert date == {"start": start, "end": end, "timezone": timezone}

    async def test_polls_the_job_until_it_finishes(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float], caplog: pytest.LogCaptureFixture
    ):
        caplog.set_level("INFO", logger="interloper_assets.teads.api")
        teads = _Teads([{"status": "pending"}, {"status": "running"}, _finished()])
        _Storage().install(monkeypatch)

        assert len(await _api(teads).report(DAY, UTC, {})) == 2
        assert len(teads.requests) == 4
        assert sleeps == [constants.REPORT_POLL_INTERVAL] * 2
        assert caplog.messages == [
            "Waiting for Teads report r-1 (pending)...",
            "Waiting for Teads report r-1 (running)...",
        ]

    async def test_a_refused_request_raises_teads_answer_without_polling(self, sleeps: list[float]):
        teads = _Teads([], run=httpx2.Response(200, json={"valid": False, "errors": ["unknown metric"]}))

        with pytest.raises(RuntimeError) as raised:
            await _api(teads).report(DAY, UTC, {})
        assert str(raised.value) == "Teads rejected the report request: {'valid': False, 'errors': ['unknown metric']}"
        assert len(teads.requests) == 1

    async def test_an_http_error_on_the_request_raises_without_polling(self, sleeps: list[float]):
        teads = _Teads([], run=httpx2.Response(401, text="Unauthorized"))

        with pytest.raises(httpx2.HTTPStatusError, match="401 Unauthorized"):
            await _api(teads).report(DAY, UTC, {})
        assert len(teads.requests) == 1

    async def test_an_http_error_on_a_poll_raises(self, sleeps: list[float]):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/api/reports/run":
                return httpx2.Response(200, json={"valid": True, "id": "r-1"})
            return httpx2.Response(404)

        with pytest.raises(httpx2.HTTPStatusError, match="404 Not Found"):
            await _api(handler).report(DAY, UTC, {})
        assert sleeps == []

    async def test_a_failed_report_raises_without_downloading(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        storage = _Storage()
        storage.install(monkeypatch)

        with pytest.raises(RuntimeError) as raised:
            await _api(_Teads([{"status": "running"}, {"status": "error"}])).report(DAY, UTC, {})
        assert str(raised.value) == "Teads report r-1 failed"
        assert storage.requests == []
        assert sleeps == [constants.REPORT_POLL_INTERVAL]

    async def test_a_report_past_the_deadline_raises(self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]):
        monkeypatch.setattr(constants, "REPORT_TIMEOUT", 0)

        with pytest.raises(RuntimeError) as raised:
            await _api(_Teads([{"status": "running"}])).report(DAY, UTC, {})
        assert str(raised.value) == "Teads report r-1 did not finish within 0s"
        assert sleeps == []

    async def test_a_report_finished_at_the_deadline_is_downloaded(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        monkeypatch.setattr(constants, "REPORT_TIMEOUT", 0)
        _Storage().install(monkeypatch)

        assert len(await _api(_Teads([_finished()])).report(DAY, UTC, {})) == 2

    async def test_a_report_failed_at_the_deadline_raises_its_failure(
        self, sleeps: list[float], monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setattr(constants, "REPORT_TIMEOUT", 0)

        with pytest.raises(RuntimeError) as raised:
            await _api(_Teads([{"status": "error"}])).report(DAY, UTC, {})
        assert str(raised.value) == "Teads report r-1 failed"

    async def test_the_download_has_its_own_client_without_the_api_key(
        self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
    ):
        storage = _Storage()
        storage.install(monkeypatch)
        client = il.AsyncRESTClient(
            constants.BASE_URL,
            headers={"Authorization": "secret"},
            transport=httpx2.MockTransport(_Teads([_finished()])),
        )

        await TeadsApi(client).report(DAY, UTC, {})

        (download,) = storage.requests
        assert "authorization" not in download.headers
        assert storage.timeouts == [constants.DOWNLOAD_TIMEOUT]

    async def test_the_download_follows_redirects(self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]):
        moved = "https://files.example/r-1.csv"
        storage = _Storage(
            {FILE: httpx2.Response(302, headers={"Location": moved}), moved: httpx2.Response(200, text=CSV)}
        )
        storage.install(monkeypatch)

        assert len(await _api(_Teads([_finished()])).report(DAY, UTC, {})) == 2
        assert _sent(storage.requests) == [("GET", FILE), ("GET", moved)]

    async def test_a_refused_download_raises(self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]):
        _Storage({FILE: httpx2.Response(403)}).install(monkeypatch)

        with pytest.raises(httpx2.HTTPStatusError, match="403 Forbidden"):
            await _api(_Teads([_finished()])).report(DAY, UTC, {})

    async def test_an_empty_report_holds_no_rows(self, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]):
        _Storage({FILE: httpx2.Response(200, text="Day,Creative ID,Clicks\n")}).install(monkeypatch)

        assert await _api(_Teads([_finished()])).report(DAY, UTC, {}) == []
