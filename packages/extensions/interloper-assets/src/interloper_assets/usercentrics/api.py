"""The Usercentrics data-export API engine every Usercentrics request goes through."""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
from typing import Any

import httpx2
import interloper as il

from interloper_assets.usercentrics.constants import DOWNLOAD_TIMEOUT

logger = logging.getLogger(__name__)


class UsercentricsApi:
    """The Usercentrics data-export API: its exports and the CSV files they span.

    An export is named ``<aggregation>-<day>`` under the analytics ID it is
    issued for, and answers with the download URLs of the CSV files that hold
    its rows; every call is scoped by that analytics ID. Usercentrics refuses
    a request with an HTTP error status, raised as it comes.
    """

    def __init__(self, client: il.AsyncRESTClient) -> None:
        """Build the engine on an authenticated client.

        Args:
            client: The client every export request goes through, carrying the connection's API key.
        """
        self.client = client

    async def export(self, analytics_id: str, aggregation: str, day: dt.date) -> list[dict[str, Any]]:
        """Read one day of an export, the rows of every file it spans.

        Args:
            analytics_id: The analytics ID the export is issued for.
            aggregation: What the export holds, such as ``granular`` or ``interaction``.
            day: The export's only day.

        Returns:
            The rows of every file, in file order, as ``csv.DictReader`` reads them (every value a string in a
            well-formed file); empty when the export has no files.
        """
        response = await self.client.get(f"/analytics/{analytics_id}/{aggregation}-{day.isoformat()}")
        response.raise_for_status()
        urls = response.json()["downloadUrls"]
        logger.info("Usercentrics %s export for %s spans %d file(s)", aggregation, day, len(urls))
        return await self._download(urls)

    @staticmethod
    async def _download(urls: list[str]) -> list[dict[str, Any]]:
        """Download an export's CSV files and parse their rows.

        The files are fetched by a client of their own, following redirects:
        the URLs may point off-host, and must never receive the API key.

        Args:
            urls: The export's download URLs.

        Returns:
            The rows of every file, in file order.
        """
        rows: list[dict[str, Any]] = []
        async with httpx2.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
            for url in urls:
                file = await client.get(url, follow_redirects=True)
                file.raise_for_status()
                rows.extend(csv.DictReader(io.StringIO(file.text)))
        return rows
