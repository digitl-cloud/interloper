from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.awin.connection import AwinConnection
from interloper_assets.awin.connector.normalizer import AwinTransactionsNormalizer
from interloper_assets.awin.connector.schemas import PublishersStats, Transactions

# -- SOURCE --------------------------------------------------------------------


@il.source(
    tags=["Affiliate"],
    icon="icon:awin",
    normalizer=DataFrameNormalizer(),
    maturity="beta",
)
class Awin(il.Source):
    """Awin affiliate network integration for transaction and publisher reporting."""

    connection: AwinConnection

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="advertiser_id",
        description="Awin advertiser account",
        discriminator=True,
    )

    @il.asset(
        schema=Transactions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
        normalizer=AwinTransactionsNormalizer(),
    )
    async def transactions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Advertiser transactions including commissions, sales amounts, and click attribution."""
        rows = await self.connection.api.rows(
            f"/advertisers/{self.advertiser_id}/transactions/",
            context.partition_date,
            timezone="UTC",
            showBasketProducts=True,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=PublishersStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def publishers_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Advertiser performance reports aggregated by publisher."""
        rows = await self.connection.api.report(
            f"/advertisers/{self.advertiser_id}/reports/publisher",
            context.partition_date,
            timezone="UTC",
        )
        return [{**row, "date": context.partition_date} for row in rows]
