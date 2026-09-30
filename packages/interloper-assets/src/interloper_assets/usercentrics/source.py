import csv
import datetime as dt
import io
import logging
from typing import Any

import httpx2
import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.usercentrics import constants, schemas
from interloper_assets.usercentrics.connection import UsercentricsConnection

logger = logging.getLogger(__name__)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Privacy & Consent"],
    icon="fluent:connector-24-filled",
    normalizer=DataFrameNormalizer(replace_empty_strings=True),
)
class Usercentrics(il.Source):
    """Usercentrics consent management analytics integration."""

    connection: UsercentricsConnection

    analytics_id: str = il.InputField(
        label="Analytics ID",
        description="Usercentrics analytics ID the data export is issued for",
        discriminator=True,
    )

    @il.asset(schema=schemas.ConsentsStats, partitioning=il.TimePartitionConfig(column="day"), tags=["Report"])
    async def consents_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily consent decisions per data processing service, settings configuration, country, OS and browser."""
        return await self._export("granular", context.partition_date)

    @il.asset(schema=schemas.InteractionsStats, partitioning=il.TimePartitionConfig(column="day"), tags=["Report"])
    async def interactions_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily consent-banner interactions per event type, settings configuration, host, country and device."""
        return await self._export("interaction", context.partition_date)

    async def _export(self, aggregation: str, date: dt.date) -> list[dict[str, Any]]:
        """Fetch one day of this analytics ID's data export and parse the rows of every file it spans.

        Shared by ``consents_stats`` (``granular``) and ``interactions_stats``
        (``interaction``): both are the same export endpoint, named
        ``<aggregation>-<day>``, answering with a list of CSV download URLs.

        Args:
            aggregation: The export kind, ``granular`` or ``interaction``.
            date: The day to export.

        Returns:
            The rows of every file, values left as strings; empty when the export has no files.
        """
        response = await self.connection.client.get(f"/analytics/{self.analytics_id}/{aggregation}-{date.isoformat()}")
        response.raise_for_status()
        urls = response.json()["downloadUrls"]
        logger.info("Usercentrics %s export for %s spans %d file(s)", aggregation, date, len(urls))

        rows: list[dict[str, Any]] = []
        # the download URLs may point off-host; never send them the API key
        async with httpx2.AsyncClient(timeout=constants.DOWNLOAD_TIMEOUT) as client:
            for url in urls:
                file = await client.get(url, follow_redirects=True)
                file.raise_for_status()
                rows.extend(csv.DictReader(io.StringIO(file.text)))
        return rows
