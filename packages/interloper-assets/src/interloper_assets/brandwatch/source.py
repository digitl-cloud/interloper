import asyncio
import datetime as dt
import logging
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.brandwatch import constants, schemas
from interloper_assets.brandwatch.connection import BrandwatchConnection

logger = logging.getLogger(__name__)

_Record = dict[str, Any]

# Insights reports queue server-side; poll until READY.
_POLL_INTERVAL = 3.0  # seconds between status polls
_POLL_TIMEOUT = 600.0  # 10 minutes


# -- HELPERS -------------------------------------------------------------------
def _clean_metric(metric_id: str) -> str:
    """Map a Measure metric id to its schema column.

    ``channel/reactions_by_type/anger/day`` -> ``reactions_by_type_anger``.
    """
    return metric_id.removeprefix("channel/").removesuffix("/day").replace("/", "_")


async def _request_insights(
    client: il.AsyncRESTClient, channel_id: str, metric_ids: list[str], start: dt.date, end: dt.date
) -> str:
    """Request a channel-insights report and return its request id."""
    response = await client.post(
        "/channel",
        json={
            "since": start.isoformat(),
            "until": end.isoformat(),
            "channelIds": [channel_id],
            "metricIds": metric_ids,
        },
    )
    response.raise_for_status()
    return response.json()["insightsRequestId"]


async def _wait_until_ready(client: il.AsyncRESTClient, request_id: str) -> None:
    """Poll a report until it is READY, raising on failure or timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _POLL_TIMEOUT
    while True:
        response = await client.get(f"/{request_id}")
        response.raise_for_status()
        status = response.json().get("status")

        if status == "READY":
            return
        if status and status != "IN_PROGRESS":
            raise RuntimeError(f"Insights request {request_id} failed with status {status}")

        if loop.time() >= deadline:
            raise RuntimeError(f"Insights request {request_id} not ready within {_POLL_TIMEOUT:.0f}s")
        await asyncio.sleep(_POLL_INTERVAL)


async def _fetch_insights_data(client: il.AsyncRESTClient, request_id: str) -> dict[str, list]:
    """Page through a ready report, merging each metric's daily points."""
    merged: dict[str, list] = {}
    page = 1
    while True:
        response = await client.get(f"/{request_id}", params={"page": page})
        response.raise_for_status()
        data = response.json()["data"]
        for metric_id, points in (data.get("insights") or {}).items():
            merged.setdefault(metric_id, []).extend(points or [])
        next_page = (data.get("paging") or {}).get("nextPage")
        if not next_page:
            break
        page = next_page
    return merged


async def _get_channel_insights(
    connection: BrandwatchConnection, channel_id: str, network: str, start: dt.date, end: dt.date
) -> dict[str, list]:
    """Fetch all of a network's metrics for a channel, batched by the per-request cap."""
    metric_ids = constants.NETWORK_METRICS[network]
    merged: dict[str, list] = {}
    for i in range(0, len(metric_ids), constants.MAX_METRICS_PER_REQUEST):
        batch = metric_ids[i : i + constants.MAX_METRICS_PER_REQUEST]
        logger.info(f"Requesting {network} insights for channel {channel_id} ({len(batch)} metrics)")
        request_id = await _request_insights(connection.client, channel_id, batch, start, end)
        await _wait_until_ready(connection.client, request_id)
        for metric_id, points in (await _fetch_insights_data(connection.client, request_id)).items():
            merged.setdefault(metric_id, []).extend(points)
    return merged


def _reshape(insights: dict[str, list], channel_id: str, fallback_date: dt.date) -> list[_Record]:
    """Pivot metric→daily-points into one row per day keyed by the schema columns.

    Each Measure point is ``{"value", "date", "channelId"}``; collect them into a
    row per date, filling ``date``/``channel_id`` once and each metric's value
    under its cleaned column name.
    """
    rows: dict[Any, _Record] = {}
    for metric_id, points in insights.items():
        column = _clean_metric(metric_id)
        for point in points or []:
            if not isinstance(point, dict):
                continue
            day = point.get("date") or fallback_date
            row = rows.setdefault(day, {"channel_id": channel_id, "date": day})
            row[column] = point.get("value")
    return list(rows.values())


# -- SOURCE --------------------------------------------------------------------
@il.source(
    resources={"connection": BrandwatchConnection},
    tags=["Social Media"],
    icon="fluent:connector-24-filled",
    normalizer=DataFrameNormalizer(),
)
class Brandwatch(il.Source):
    """Brandwatch (Falcon.io) social media analytics integration."""

    channel_id: str = il.InputField(
        title="Channel ID",
        description="Falcon.io channel id (a connected social profile)",
        discriminator=True,
    )

    async def _network_stats(
        self, context: il.ExecutionContext, connection: BrandwatchConnection, network: str
    ) -> list[_Record]:
        insights = await _get_channel_insights(
            connection, self.channel_id, network, context.partition_date, context.partition_date
        )
        return _reshape(insights, self.channel_id, context.partition_date)

    @il.asset(
        schema=schemas.FacebookStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def facebook_stats(self, context: il.ExecutionContext, connection: BrandwatchConnection) -> list[_Record]:
        """Facebook channel performance metrics per day."""
        return await self._network_stats(context, connection, "facebook")

    @il.asset(
        schema=schemas.InstagramStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def instagram_stats(self, context: il.ExecutionContext, connection: BrandwatchConnection) -> list[_Record]:
        """Instagram channel performance metrics per day."""
        return await self._network_stats(context, connection, "instagram")

    @il.asset(
        schema=schemas.LinkedinStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def linkedin_stats(self, context: il.ExecutionContext, connection: BrandwatchConnection) -> list[_Record]:
        """LinkedIn channel performance metrics per day."""
        return await self._network_stats(context, connection, "linkedin")

    @il.asset(
        schema=schemas.TwitterStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def twitter_stats(self, context: il.ExecutionContext, connection: BrandwatchConnection) -> list[_Record]:
        """X (Twitter) channel performance metrics per day."""
        return await self._network_stats(context, connection, "twitter")

    @il.asset(
        schema=schemas.YoutubeStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def youtube_stats(self, context: il.ExecutionContext, connection: BrandwatchConnection) -> list[_Record]:
        """YouTube channel performance metrics per day."""
        return await self._network_stats(context, connection, "youtube")
