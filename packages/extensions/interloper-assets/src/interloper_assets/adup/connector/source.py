from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.adup import constants
from interloper_assets.adup.connection import AdupConnection
from interloper_assets.adup.connector.schemas import Account, AdsStats


@il.source(
    tags=["Advertising"],
    icon="icon:adup",
    normalizer=DataFrameNormalizer(),
    maturity="beta",
)
class Adup(il.Source):
    """Adup advertising platform integration."""

    connection: AdupConnection

    @il.asset(
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        schema=Account,
    )
    async def account(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Advertiser account information."""
        account = await self.connection.api.get("/advertisers/me")
        return [{**account, "date": context.partition_date}]

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
        tags=["Report"],
        schema=AdsStats,
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad performance insights with metrics like impressions, clicks, conversions, and cost."""
        return await self.connection.api.report(
            context.window.start,
            context.window.end,
            report_name="AD_PERFORMANCE_REPORT",
            report_type="AD_PERFORMANCE_REPORT",
            select=constants.FIELDS["AD_PERFORMANCE_REPORT"],
            conditions=[],
        )
