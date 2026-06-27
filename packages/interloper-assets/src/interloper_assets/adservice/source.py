import datetime as dt
import logging
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.adservice.connection import AdserviceConnection
from interloper_assets.adservice.schemas import (
    CampaignsStats,
    CampaignsStatsByBrowser,
    CampaignsStatsByCity,
    CampaignsStatsByDeviceType,
    Conversions,
    ConversionsStatsByTimeOfDay,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------


async def get_report(
    client: il.AsyncRESTClient,
    start_date: dt.date,
    end_date: dt.date,
    report_type: str,
    group_by: str | None = None,
    end_group: str | None = None,
    sales_amount: int | None = None,
) -> dict:
    """Fetch a report from the Adservice API."""
    response = await client.get(
        url=f"/{report_type}",
        params={
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "sales_amount": sales_amount,
            "group_by": group_by,
            "end_group": end_group,
        },
    )
    response.raise_for_status()
    return response.json()


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": AdserviceConnection},
    tags=["Advertising"],
    icon="carbon:analytics",
    # Adservice returns snake_case fields already; the normalizer coerces to a frame.
    normalizer=DataFrameNormalizer(),
)
class Adservice(il.Source):
    """Adservice advertising platform integration."""

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats(
        self, context: il.ExecutionContext, connection: AdserviceConnection
    ) -> list[dict[str, Any]]:
        """Campaign performance statistics with metrics like impressions, clicks, and conversions."""
        response = await get_report(
            client=connection.client,
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics",
            group_by="stamp,camp_id",
            end_group="stamp",
            sales_amount=1,
        )
        return response["data"]["rows"]

    @il.asset(
        schema=Conversions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def conversions(self, context: il.ExecutionContext, connection: AdserviceConnection) -> list[dict[str, Any]]:
        """Conversion events and attribution data."""
        response = await get_report(
            client=connection.client,
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="conversions",
        )
        return response["data"]

    @il.asset(
        schema=ConversionsStatsByTimeOfDay,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def conversions_stats_by_time_of_day(
        self, context: il.ExecutionContext, connection: AdserviceConnection
    ) -> list[dict[str, Any]]:
        """Conversion events broken down by time of day."""
        response = await get_report(
            client=connection.client,
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/conversions/timeofday",
        )
        return response["data"]

    @il.asset(
        schema=CampaignsStatsByCity,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats_by_city(
        self, context: il.ExecutionContext, connection: AdserviceConnection
    ) -> list[dict[str, Any]]:
        """Campaign performance segmented by city."""
        response = await get_report(
            client=connection.client,
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="city",
        )
        return response["data"]

    @il.asset(
        schema=CampaignsStatsByBrowser,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats_by_browser(
        self, context: il.ExecutionContext, connection: AdserviceConnection
    ) -> list[dict[str, Any]]:
        """Campaign performance segmented by browser."""
        response = await get_report(
            client=connection.client,
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="browser",
        )
        return response["data"]

    @il.asset(
        schema=CampaignsStatsByDeviceType,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats_by_device_type(
        self, context: il.ExecutionContext, connection: AdserviceConnection
    ) -> list[dict[str, Any]]:
        """Campaign performance segmented by device type."""
        response = await get_report(
            client=connection.client,
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="device_type",
        )
        return response["data"]
