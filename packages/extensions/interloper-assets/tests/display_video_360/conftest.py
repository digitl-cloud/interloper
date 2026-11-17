"""Shared Google discovery and report download fakes.

Every Display & Video 360 request goes through a Google discovery service, so
the fake builds the real services from the discovery documents the client
library ships, over a transport that serves scripted answers: resource paths,
parameter checks and URLs stay real, only the wire is faked. A Bid Manager
report file is downloaded from a signed storage URL with ``httpx2.get``, which
the download fake replaces.
"""

from __future__ import annotations

import json
from typing import Any

import httplib2
import httpx2
import pytest
from googleapiclient import discovery

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection


class FakeGoogle:
    """Record the requests both APIs send and serve scripted answers, in order.

    An answer is a JSON body served with status 200, or a ``(status, body)``
    pair. Once the script runs out every further request gets ``{}``, so a
    test only scripts what it reads.
    """

    def __init__(self) -> None:
        """Start with an empty script, no recorded requests and no services built."""
        self.requests: list[tuple[str, str, Any]] = []
        self.builds: list[str] = []
        self._script: list[Any] = []

    def serve(self, *answers: Any) -> None:
        """Script the answers to the next requests.

        Args:
            *answers: One per request, in order: a JSON body, or a ``(status, body)`` pair.
        """
        self._script.extend(answers)

    def display_video(self) -> Any:
        """Build a Display & Video API service, as the connection's ``dv_client`` does.

        Returns:
            The service, sending its requests through this fake.
        """
        return self._build(constants.DV_API_SERVICE, constants.DV_API_VERSION)

    def bid_manager(self) -> Any:
        """Build a Bid Manager API service, as the connection's ``dbm_client`` does.

        Returns:
            The service, sending its requests through this fake.
        """
        return self._build(constants.DBM_API_SERVICE, constants.DBM_API_VERSION)

    def request(self, uri: str, method: str = "GET", body: str | None = None, **kwargs: Any) -> tuple[Any, bytes]:
        """Record one request and answer it with the next scripted answer, as ``httplib2.Http.request`` would.

        Args:
            uri: The request's full URL, query string included.
            method: The HTTP method.
            body: The JSON body, if the request has one.
            **kwargs: The transport options the client passes (headers, redirections), ignored.

        Returns:
            The response's status and its JSON content.
        """
        self.requests.append((method, uri, json.loads(body) if body else None))
        answer = self._script.pop(0) if self._script else {}
        status, content = answer if isinstance(answer, tuple) else (200, answer)
        return httplib2.Response({"status": status}), json.dumps(content).encode()

    def _build(self, name: str, version: str) -> Any:
        """Build a service of one API over this fake, recording the build.

        Args:
            name: The API's discovery name.
            version: The API's version.

        Returns:
            The service.
        """
        self.builds.append(name)
        return discovery.build(name, version, http=self, static_discovery=True)


class FakeDownload:
    """Serve a report file at any URL, recording each download."""

    def __init__(self, text: str) -> None:
        """Serve ``text`` with status 200 until told otherwise.

        Args:
            text: The report file's content.
        """
        self.text = text
        self.status = 200
        self.downloads: list[dict[str, Any]] = []

    def get(self, url: str, timeout: Any) -> httpx2.Response:
        """Record one download and serve the file, as ``httpx2.get`` would.

        Args:
            url: The file's signed storage URL.
            timeout: The timeout the caller asked for.

        Returns:
            The file, encoded with a byte order mark as Bid Manager serves it.
        """
        self.downloads.append({"url": url, "timeout": timeout})
        return httpx2.Response(self.status, content=self.text.encode("utf-8-sig"), request=httpx2.Request("GET", url))


@pytest.fixture
def google() -> FakeGoogle:
    """Fake both Google APIs.

    Returns:
        The fake, holding the requests sent and the services built.
    """
    return FakeGoogle()


@pytest.fixture
def connection(google: FakeGoogle) -> DisplayVideo360Connection:
    """A connection whose services are built over the fake.

    Args:
        google: The fake both APIs answer through.

    Returns:
        The connection.
    """
    connection = DisplayVideo360Connection(service_account_key="{}")
    connection.__dict__["dv_client"] = google.display_video
    connection.__dict__["dbm_client"] = google.bid_manager
    return connection


@pytest.fixture
def download(monkeypatch: pytest.MonkeyPatch) -> FakeDownload:
    """Serve a two-row report file, with its summary footer, in place of the signed storage URL.

    Args:
        monkeypatch: The pytest monkeypatch fixture that restores ``httpx2.get`` after the test.

    Returns:
        The fake download, whose ``text`` and ``status`` a test may change.
    """
    fake = FakeDownload(
        "Date,Advertiser ID,Active View: Viewable Impressions,Active View: % Viewable Impressions,Impressions\n"
        "2026/07/10,9,90,85.5,100\n"
        "2026/07/10,9,180,72.1,200\n"
        ",,270,,300\n"
        "\n"
        "Report Time:,2026/07/11\n"
    )
    monkeypatch.setattr(httpx2, "get", fake.get)
    return fake
