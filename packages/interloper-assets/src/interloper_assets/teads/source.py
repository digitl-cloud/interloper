import asyncio
import csv
import datetime as dt
import io
import logging
from typing import Any

import httpx2
import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.teads import constants, schemas
from interloper_assets.teads.connection import TeadsConnection

logger = logging.getLogger(__name__)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:teads",
    normalizer=DataFrameNormalizer(replace_empty_strings=True),
    maturity="beta",
)
class Teads(il.Source):
    """Teads advertising platform integration."""

    connection: TeadsConnection

    advertiser_id: int = il.InputField(
        label="Advertiser ID",
        description="Teads advertiser the reports are filtered to",
        discriminator=True,
    )

    @il.asset(schema=schemas.CreativesStats, partitioning=il.TimePartitionConfig(column="day"), tags=["Report"])
    async def creatives_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily delivery, spend, click and video metrics per creative, campaign and line item."""
        return await self._report(context.partition_date)

    async def _report(self, date: dt.date) -> list[dict[str, Any]]:
        """Run the standard report for one day and return its CSV rows.

        Requests the report job filtered to this source's advertiser, polls its
        status every ``REPORT_POLL_INTERVAL`` seconds until it finishes, then
        downloads the CSV the finished status points at.

        Args:
            date: The day to report on, as a UTC day.

        Returns:
            The report rows keyed by Teads' display-name headers, values left as strings.

        Raises:
            RuntimeError: If Teads rejects the report request, the report fails, or it
                does not finish within ``REPORT_TIMEOUT`` seconds.
        """
        # request the report job
        day = date.isoformat()
        response = await self.connection.client.post(
            "/api/reports/run",
            json={
                "filters": {
                    "date": {
                        "start": f"{day}T00:00:00.000+00:00",
                        "end": f"{day}T23:59:59.999+00:00",
                        "timezone": constants.REPORT_TIMEZONE,
                    },
                    "advertisers": [self.advertiser_id],
                },
                "dimensions": constants.STANDARD_DIMENSIONS,
                "metrics": constants.STANDARD_METRICS,
            },
        )
        response.raise_for_status()
        job = response.json()
        if not job.get("valid"):
            raise RuntimeError(f"Teads rejected the report request: {job}")
        report_id = job["id"]

        # poll until finished
        loop = asyncio.get_running_loop()
        deadline = loop.time() + constants.REPORT_TIMEOUT
        while True:
            response = await self.connection.client.get(f"/api/reports/status/{report_id}")
            response.raise_for_status()
            status = response.json()
            if status["status"] == "finished":
                break
            if status["status"] == "error":
                raise RuntimeError(f"Teads report {report_id} failed")
            if loop.time() >= deadline:
                raise RuntimeError(f"Teads report {report_id} did not finish within {constants.REPORT_TIMEOUT}s")
            logger.info("Waiting for Teads report %s (%s)...", report_id, status["status"])
            await asyncio.sleep(constants.REPORT_POLL_INTERVAL)

        # the report URI may point off-host; never send it the API key
        async with httpx2.AsyncClient(timeout=constants.DOWNLOAD_TIMEOUT) as client:
            response = await client.get(status["uri"], follow_redirects=True)
        response.raise_for_status()
        return list(csv.DictReader(io.StringIO(response.text)))
