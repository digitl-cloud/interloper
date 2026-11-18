from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.adtraction.connection import AdtractionConnection
from interloper_assets.adtraction.connector.schemas import (
    AdsStats,
    ChannelsStats,
    Programs,
    ProgramStats,
    SegmentsStats,
    Transactions,
)

# Adtraction's transactionStatus codes, which a statistic counts and a transaction list holds
APPROVED_AND_PENDING = 3
OPEN_CLAIMS = 4
REJECTED = 5


@il.source(
    tags=["Affiliate"],
    icon="icon:adtraction",
    # a transaction nests the click that preceded it; it becomes click_* columns
    normalizer=DataFrameNormalizer(flatten_max_level=1),
    maturity="beta",
)
class Adtraction(il.Source):
    """Adtraction affiliate network integration for an advertiser's partner program, its statistics and transactions.

    An Adtraction API token belongs to one advertiser account, which runs one partner program.
    """

    connection: AdtractionConnection

    @il.asset(
        schema=Programs,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    async def programs(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The advertiser's partner program: its name, currency and market."""
        program = await self.connection.api.get("/advertiser/account/")
        return [{**program, "date": context.partition_date}]

    @il.asset(
        schema=ProgramStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def program_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily partner program performance, counting approved and pending transactions."""
        return await self.connection.api.report(
            "/advertiser/statistics/days/", context.partition_date, transactionStatus=APPROVED_AND_PENDING
        )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily performance of each banner or link, counting approved and pending transactions."""
        rows = await self.connection.api.report(
            "/advertiser/statistics/ads/", context.partition_date, transactionStatus=APPROVED_AND_PENDING
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=ChannelsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def channels_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily performance of each partner channel, counting approved and pending transactions."""
        rows = await self.connection.api.report(
            "/advertiser/statistics/channels/", context.partition_date, transactionStatus=APPROVED_AND_PENDING
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=SegmentsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def segments_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily performance of each commission segment, counting approved and pending transactions."""
        rows = await self.connection.api.report(
            "/advertiser/statistics/segments/", context.partition_date, transactionStatus=APPROVED_AND_PENDING
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Transactions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def transactions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Every transaction registered on the day, whatever its status, with the click that preceded it."""
        rows = []
        for status in (APPROVED_AND_PENDING, OPEN_CLAIMS, REJECTED):
            rows += await self.connection.api.rows(
                "/advertiser/transactions/", context.partition_date, transactionStatus=status
            )
        return [{**row, "date": context.partition_date} for row in rows]
