from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.adservice.connection import AdserviceConnection
from interloper_assets.adservice.connector.schemas import (
    CampaignsStats,
    CampaignsStatsByBrowser,
    CampaignsStatsByCity,
    CampaignsStatsByDeviceType,
    Conversions,
    ConversionsStatsByTimeOfDay,
)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="carbon:analytics",
    # Adservice returns snake_case fields already; the normalizer coerces to a frame.
    normalizer=DataFrameNormalizer(),
    maturity="beta",
)
class Adservice(il.Source):
    """Adservice advertising platform integration."""

    connection: AdserviceConnection

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance statistics with metrics like impressions, clicks, and conversions."""
        statistics = await self.connection.api.report(
            "/statistics", context.partition_date, sales_amount=1, group_by="stamp,camp_id", end_group="stamp"
        )
        return statistics["rows"]

    @il.asset(
        schema=Conversions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def conversions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Conversion events and attribution data."""
        rows = await self.connection.api.report("/conversions", context.partition_date)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=ConversionsStatsByTimeOfDay,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def conversions_stats_by_time_of_day(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Conversion events broken down by time of day."""
        rows = await self.connection.api.report("/statistics/conversions/timeofday", context.partition_date)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=CampaignsStatsByCity,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats_by_city(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance segmented by city."""
        rows = await self.connection.api.report("/statistics/devicedetails", context.partition_date, group_by="city")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=CampaignsStatsByBrowser,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats_by_browser(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance segmented by browser."""
        rows = await self.connection.api.report("/statistics/devicedetails", context.partition_date, group_by="browser")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=CampaignsStatsByDeviceType,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats_by_device_type(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance segmented by device type."""
        rows = await self.connection.api.report(
            "/statistics/devicedetails", context.partition_date, group_by="device_type"
        )
        return [{**row, "date": context.partition_date} for row in rows]
