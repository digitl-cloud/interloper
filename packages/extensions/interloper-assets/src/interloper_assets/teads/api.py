"""The Teads API engine every Teads request goes through."""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
from asyncio import get_running_loop, sleep
from typing import Any
from zoneinfo import ZoneInfo

import httpx2
import interloper as il

from interloper_assets.teads import constants

logger = logging.getLogger(__name__)


class TeadsApi:
    """The Teads API: its report jobs.

    A report is a job: requested from ``/api/reports/run``, polled until it
    finishes, then downloaded as a CSV from the URI its finished status names.
    Teads refuses a report it cannot run by answering ``valid: false``, raised
    with its whole answer, and any request with an HTTP error status, raised as
    it comes.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every API request goes through, carrying the connection's API key.
        """
        self.client = client

    async def report(self, day: dt.date, timezone: str, filters: dict[str, Any], **body: Any) -> list[dict[str, Any]]:
        """Read one day of any report: request its job, wait for it to finish, download its rows.

        Args:
            day: The report's only day.
            timezone: The IANA name of the timezone the day is read in, such as ``Etc/GMT+0``.
            filters: The report's filters beside its day, such as its ``advertisers``; the
                ``date`` filter is the engine's, built from ``day`` and ``timezone``.
            **body: What the report is, in Teads' names: its ``dimensions`` and ``metrics``.

        Returns:
            The report's CSV rows, keyed by Teads' display-name headers, values left as strings.
        """
        report_id = await self._run({"filters": {"date": self._window(day, timezone), **filters}, **body})
        uri = await self._wait(report_id)
        return await self._download(uri)

    async def _run(self, body: dict[str, Any]) -> int | str:
        """Request a report job.

        Args:
            body: The report's request, its day already in its filters.

        Returns:
            The job's id, as Teads names it.

        Raises:
            RuntimeError: If Teads answers that the request is not valid.
        """
        response = await self.client.post("/api/reports/run", json=body)
        response.raise_for_status()
        job = response.json()
        if not job.get("valid"):
            raise RuntimeError(f"Teads rejected the report request: {job}")
        return job["id"]

    async def _wait(self, report_id: int | str) -> str:
        """Poll a report job until it finishes.

        Each poll depends on the previous one's status, so the polls run one
        after another, ``REPORT_POLL_INTERVAL`` seconds apart, until
        ``REPORT_TIMEOUT``.

        Args:
            report_id: The job's id.

        Returns:
            The URI of the finished report's CSV.

        Raises:
            RuntimeError: If the report fails, or does not finish within ``REPORT_TIMEOUT`` seconds.
        """
        loop = get_running_loop()
        deadline = loop.time() + constants.REPORT_TIMEOUT
        while True:
            response = await self.client.get(f"/api/reports/status/{report_id}")
            response.raise_for_status()
            status = response.json()
            if status["status"] == "finished":
                return status["uri"]
            if status["status"] == "error":
                raise RuntimeError(f"Teads report {report_id} failed")
            if loop.time() >= deadline:
                raise RuntimeError(f"Teads report {report_id} did not finish within {constants.REPORT_TIMEOUT}s")
            logger.info("Waiting for Teads report %s (%s)...", report_id, status["status"])
            await sleep(constants.REPORT_POLL_INTERVAL)

    @staticmethod
    async def _download(uri: str) -> list[dict[str, Any]]:
        """Download a finished report's CSV.

        The URI may point off the API's host, so the file is fetched by a
        client of its own, which never sends it the API key.

        Args:
            uri: The URI the finished report's status names; a redirect is followed.

        Returns:
            The CSV's rows, keyed by its header row, values left as strings.
        """
        async with httpx2.AsyncClient(timeout=constants.DOWNLOAD_TIMEOUT) as storage:
            response = await storage.get(uri, follow_redirects=True)
        response.raise_for_status()
        return list(csv.DictReader(io.StringIO(response.text)))

    @staticmethod
    def _window(day: dt.date, timezone: str) -> dict[str, str]:
        """Encode a day as Teads' date filter over it.

        Args:
            day: The day to cover.
            timezone: The IANA name of the timezone the day is read in.

        Returns:
            The filter's first and last millisecond of the day, each with its offset in
            ``timezone``, and the timezone Teads reads them in.
        """
        zone = ZoneInfo(timezone)
        start = dt.datetime.combine(day, dt.time.min, zone)
        end = dt.datetime.combine(day, dt.time.max, zone)
        return {
            "start": start.isoformat(timespec="milliseconds"),
            "end": end.isoformat(timespec="milliseconds"),
            "timezone": timezone,
        }
