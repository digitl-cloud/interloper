"""Shared fakes of both Google APIs and of the report download.

Every Display & Video 360 request goes through the connection's client, so the
fake is that client's transport: it records each request to either API and
serves scripted answers, and serves a Bid Manager report file at its signed
storage URL.
"""

from __future__ import annotations

import json
from typing import Any

import httpx2
import interloper as il
import pytest

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection

TOKEN = "Bearer service-account-token"


class FakeDownload:
    """Serve a report file at its signed storage URL, recording each download."""

    def __init__(self, text: str) -> None:
        """Serve ``text`` with status 200 until told otherwise.

        Args:
            text: The report file's content.
        """
        self.text = text
        self.status = 200
        self.downloads: list[dict[str, Any]] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Record one download and serve the file.

        Args:
            request: The download request.

        Returns:
            The file, encoded with a byte order mark as Bid Manager serves it.
        """
        self.downloads.append(
            {
                "url": str(request.url),
                "authorization": request.headers.get("Authorization"),
                "timeout": request.extensions["timeout"]["read"],
            }
        )
        return httpx2.Response(self.status, content=self.text.encode("utf-8-sig"))


class FakeGoogle:
    """Record the requests both APIs receive and serve scripted answers, in order.

    An answer is a JSON body served with status 200, or a ``(status, body)``
    pair. Once the script runs out every further request gets ``{}``, so a
    test only scripts what it reads. A request to the storage host is a report
    download, served by ``download`` and recorded there.
    """

    def __init__(self, download: FakeDownload) -> None:
        """Start with an empty script and no recorded requests.

        Args:
            download: Serves the report files.
        """
        self.download = download
        self.requests: list[tuple[str, str, Any]] = []
        self.client = il.AsyncRESTClient(
            constants.DISPLAY_VIDEO_URL,
            auth=il.HTTPBearerAuth("service-account-token"),
            transport=httpx2.MockTransport(self),
        )
        self._script: list[Any] = []

    def serve(self, *answers: Any) -> None:
        """Script the answers to the next requests.

        Args:
            *answers: One per request, in order: a JSON body, or a ``(status, body)`` pair.
        """
        self._script.extend(answers)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        """Record one request and answer it with the next scripted answer.

        Args:
            request: The request, to either API or to the storage host.

        Returns:
            The answer.
        """
        if request.url.host == "storage":
            return self.download(request)
        assert request.headers["Authorization"] == TOKEN
        self.requests.append(
            (request.method, str(request.url), json.loads(request.content) if request.content else None)
        )
        answer = self._script.pop(0) if self._script else {}
        status, content = answer if isinstance(answer, tuple) else (200, answer)
        return httpx2.Response(status, json=content)


@pytest.fixture
def download() -> FakeDownload:
    """Serve a two-row report file, with its summary footer.

    Returns:
        The fake download, whose ``text`` and ``status`` a test may change.
    """
    return FakeDownload(
        "Date,Advertiser ID,Active View: Viewable Impressions,Active View: % Viewable Impressions,Impressions\n"
        "2026/07/10,9,90,85.5,100\n"
        "2026/07/10,9,180,72.1,200\n"
        ",,270,,300\n"
        "\n"
        "Report Time:,2026/07/11\n"
    )


@pytest.fixture
def google(download: FakeDownload) -> FakeGoogle:
    """Fake both Google APIs and the storage host.

    Args:
        download: Serves the report files.

    Returns:
        The fake, holding the requests sent and the client sending them.
    """
    return FakeGoogle(download)


@pytest.fixture
def connection(google: FakeGoogle) -> DisplayVideo360Connection:
    """A connection whose client sends its requests to the fake.

    Args:
        google: The fake both APIs answer through.

    Returns:
        The connection.
    """
    connection = DisplayVideo360Connection(service_account_key="{}")
    connection.__dict__["client"] = google.client
    return connection
