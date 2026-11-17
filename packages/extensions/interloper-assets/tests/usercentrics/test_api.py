"""Tests for the Usercentrics data-export API engine.

An export is named ``<aggregation>-<day>`` under its analytics ID and answers
with the download URLs of the CSV files holding its rows. The files may live
off-host, so they are fetched by a client of their own that never carries the
API key. These tests pin both halves against mock transports.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.usercentrics.api import UsercentricsApi
from interloper_assets.usercentrics.constants import BASE_URL, DOWNLOAD_TIMEOUT

FIRST_CSV = "day,dps_id,total\n2026-06-14,HkocEodjb7,20\n"
SECOND_CSV = "day,dps_id,total\n2026-06-14,S1pcEj_jZX,7\n2026-06-14,BJz7qNsdj-7,3\n"


class ExportService:
    """The export endpoint, answering every export with the same download URLs and recording each request."""

    def __init__(self, urls: list[str]) -> None:
        """Answer with the given download URLs.

        Args:
            urls: The download URLs every export answers with.
        """
        self.urls = urls
        self.requests: list[httpx2.Request] = []

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        """Answer an export request with the download URLs.

        Args:
            request: The export request.

        Returns:
            A success response listing ``urls`` as ``downloadUrls``.
        """
        self.requests.append(request)
        return httpx2.Response(200, json={"downloadUrls": self.urls})


class Storage:
    """The files an export's download URLs point to, recording every request and client opened to fetch them."""

    def __init__(self) -> None:
        """Start with no files, requests or clients."""
        self.files: dict[str, httpx2.Response] = {}
        self.requests: list[httpx2.Request] = []
        self.clients: list[dict[str, Any]] = []

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        """Answer a download with the file served at its URL.

        Args:
            request: The download request.

        Returns:
            The response registered for the request's URL.
        """
        self.requests.append(request)
        return self.files[str(request.url)]


@pytest.fixture
def storage(monkeypatch: pytest.MonkeyPatch) -> Storage:
    """Serve downloads from a `Storage`, in place of every client the engine opens to fetch files.

    Args:
        monkeypatch: Replaces ``httpx2.AsyncClient`` for the test.

    Returns:
        The storage the test fills with files and reads requests and clients from.
    """
    storage = Storage()
    real_client = httpx2.AsyncClient

    def client(**kwargs: Any) -> httpx2.AsyncClient:
        storage.clients.append(kwargs)
        return real_client(transport=httpx2.MockTransport(storage.handle), **kwargs)

    monkeypatch.setattr(httpx2, "AsyncClient", client)
    return storage


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> UsercentricsApi:
    return UsercentricsApi(
        il.AsyncRESTClient(BASE_URL, headers={"X-API-Key": "secret"}, transport=httpx2.MockTransport(handler))
    )


class TestExport:
    async def test_reads_the_export_named_for_its_aggregation_and_day(self, storage: Storage):
        service = ExportService([])

        await _api(service.handle).export("an-1", "interaction", dt.date(2026, 6, 14))

        (request,) = service.requests
        assert request.method == "GET"
        assert str(request.url) == f"{BASE_URL}/analytics/an-1/interaction-2026-06-14"
        assert request.content == b""
        assert request.headers["x-api-key"] == "secret"

    async def test_returns_the_rows_of_every_file_in_order_as_strings(self, storage: Storage):
        urls = ["https://storage.example/1.csv", "https://storage.example/2.csv"]
        storage.files = {urls[0]: httpx2.Response(200, text=FIRST_CSV), urls[1]: httpx2.Response(200, text=SECOND_CSV)}

        rows = await _api(ExportService(urls).handle).export("an-1", "granular", dt.date(2026, 6, 14))

        assert rows == [
            {"day": "2026-06-14", "dps_id": "HkocEodjb7", "total": "20"},
            {"day": "2026-06-14", "dps_id": "S1pcEj_jZX", "total": "7"},
            {"day": "2026-06-14", "dps_id": "BJz7qNsdj-7", "total": "3"},
        ]
        assert [str(request.url) for request in storage.requests] == urls

    async def test_files_download_through_one_client_without_the_api_key(self, storage: Storage):
        urls = ["https://storage.example/1.csv", "https://storage.example/2.csv"]
        storage.files = {urls[0]: httpx2.Response(200, text=FIRST_CSV), urls[1]: httpx2.Response(200, text=SECOND_CSV)}

        await _api(ExportService(urls).handle).export("an-1", "granular", dt.date(2026, 6, 14))

        assert storage.clients == [{"timeout": DOWNLOAD_TIMEOUT}]
        assert [(request.method, str(request.url)) for request in storage.requests] == [("GET", url) for url in urls]
        assert all("x-api-key" not in request.headers for request in storage.requests)

    async def test_a_download_follows_redirects(self, storage: Storage):
        url = "https://storage.example/1.csv"
        storage.files = {
            url: httpx2.Response(302, headers={"Location": "https://cdn.example/1.csv"}),
            "https://cdn.example/1.csv": httpx2.Response(200, text=FIRST_CSV),
        }

        rows = await _api(ExportService([url]).handle).export("an-1", "granular", dt.date(2026, 6, 14))

        assert [row["dps_id"] for row in rows] == ["HkocEodjb7"]
        assert [str(request.url) for request in storage.requests] == [url, "https://cdn.example/1.csv"]

    async def test_an_export_without_files_has_no_rows(self, storage: Storage):
        assert await _api(ExportService([]).handle).export("an-1", "granular", dt.date(2026, 6, 14)) == []
        assert storage.requests == []

    async def test_an_empty_file_has_no_rows(self, storage: Storage):
        url = "https://storage.example/1.csv"
        storage.files = {url: httpx2.Response(200, text="")}

        assert await _api(ExportService([url]).handle).export("an-1", "granular", dt.date(2026, 6, 14)) == []

    async def test_a_refused_export_raises_before_any_download(self, storage: Storage):
        api = _api(lambda _: httpx2.Response(403, json={"message": "Forbidden"}))

        with pytest.raises(httpx2.HTTPStatusError):
            await api.export("an-1", "granular", dt.date(2026, 6, 14))
        assert storage.requests == []

    async def test_a_failed_download_raises(self, storage: Storage):
        url = "https://storage.example/1.csv"
        storage.files = {url: httpx2.Response(404)}

        with pytest.raises(httpx2.HTTPStatusError):
            await _api(ExportService([url]).handle).export("an-1", "granular", dt.date(2026, 6, 14))

    async def test_logs_how_many_files_the_export_spans(self, storage: Storage, caplog: pytest.LogCaptureFixture):
        urls = ["https://storage.example/1.csv", "https://storage.example/2.csv"]
        storage.files = {urls[0]: httpx2.Response(200, text=""), urls[1]: httpx2.Response(200, text="")}

        with caplog.at_level(logging.INFO, logger="interloper_assets.usercentrics.api"):
            await _api(ExportService(urls).handle).export("an-1", "granular", dt.date(2026, 6, 14))

        assert "Usercentrics granular export for 2026-06-14 spans 2 file(s)" in caplog.messages
