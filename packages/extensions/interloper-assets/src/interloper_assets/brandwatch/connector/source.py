import datetime as dt
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.brandwatch import constants
from interloper_assets.brandwatch.connection import BrandwatchConnection
from interloper_assets.brandwatch.connector import schemas

_Record = dict[str, Any]


# -- HELPERS -------------------------------------------------------------------
def _clean_metric(metric_id: str) -> str:
    """Map a Measure metric id to its schema column.

    ``channel/reactions_by_type/anger/day`` -> ``reactions_by_type_anger``.

    Args:
        metric_id: The metric as Measure names it.

    Returns:
        The column the metric's values land in.
    """
    return metric_id.removeprefix("channel/").removesuffix("/day").replace("/", "_")


def _reshape(insights: dict[str, list], channel_id: str, fallback_date: dt.date) -> list[_Record]:
    """Pivot metric→daily-points into one row per day keyed by the schema columns.

    Each Measure point is ``{"value", "date", "channelId"}``; collect them into a
    row per date, filling ``date``/``channel_id`` once and each metric's value
    under its cleaned column name. Every asset pivots its insights here rather
    than in a normalizer, which never sees the channel and day it asked for.

    Args:
        insights: Each metric's points by metric id, as the engine's ``report`` returns them.
        channel_id: The channel the insights were asked for, filled on every row.
        fallback_date: The day asked for, filled on a point that carries no ``date``.

    Returns:
        One row per day, in the order the days first appear.
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
    tags=["Social Media"],
    icon="fluent:connector-24-filled",
    normalizer=DataFrameNormalizer(),
    maturity="beta",
)
class Brandwatch(il.Source):
    """Brandwatch (Falcon.io) social media analytics integration."""

    connection: BrandwatchConnection

    channel_id: str = il.InputField(
        label="Channel ID",
        description="Falcon.io channel id (a connected social profile)",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.FacebookStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def facebook_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Facebook channel performance metrics per day."""
        insights = await self.connection.api.report(
            "/channel", context.partition_date, constants.FACEBOOK_METRICS, channelIds=[self.channel_id]
        )
        return _reshape(insights, self.channel_id, context.partition_date)

    @il.asset(
        schema=schemas.InstagramStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def instagram_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Instagram channel performance metrics per day."""
        insights = await self.connection.api.report(
            "/channel", context.partition_date, constants.INSTAGRAM_METRICS, channelIds=[self.channel_id]
        )
        return _reshape(insights, self.channel_id, context.partition_date)

    @il.asset(
        schema=schemas.LinkedinStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def linkedin_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """LinkedIn channel performance metrics per day."""
        insights = await self.connection.api.report(
            "/channel", context.partition_date, constants.LINKEDIN_METRICS, channelIds=[self.channel_id]
        )
        return _reshape(insights, self.channel_id, context.partition_date)

    @il.asset(
        schema=schemas.TwitterStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def twitter_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """X (Twitter) channel performance metrics per day."""
        insights = await self.connection.api.report(
            "/channel", context.partition_date, constants.TWITTER_METRICS, channelIds=[self.channel_id]
        )
        return _reshape(insights, self.channel_id, context.partition_date)

    @il.asset(
        schema=schemas.YoutubeStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def youtube_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """YouTube channel performance metrics per day."""
        insights = await self.connection.api.report(
            "/channel", context.partition_date, constants.YOUTUBE_METRICS, channelIds=[self.channel_id]
        )
        return _reshape(insights, self.channel_id, context.partition_date)
