from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_ads_360 import constants
from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.connector import schemas


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="search_ads_360",
    name="Search Ads 360",
    tags=["Advertising"],
    icon="devicon:google",
    normalizer=DataFrameNormalizer(flatten_max_level=3, drop_na_columns=True),
    maturity="beta",
)
class SearchAds360(il.Source):
    """Search Ads 360 advertising platform integration."""

    connection: SearchAds360Connection

    manager_customer_id: str = il.InputField(description="SA360 manager account customer ID")
    customer_client_id: str = il.InputField(description="SA360 customer client ID to report on", discriminator=True)

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="segments_date"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance metrics per day."""
        return await self.connection.api.report(
            self.customer_client_id,
            context.partition_date,
            resource="campaign",
            fields=constants.CAMPAIGN_FIELDS,
            login_customer_id=self.manager_customer_id,
        )

    @il.asset(
        schema=schemas.CustomerClients,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    async def customer_clients(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All client accounts, direct and indirect, under the manager account."""
        rows = await self.connection.api.search(
            self.manager_customer_id, resource="customer_client", fields=constants.CUSTOMER_CLIENT_FIELDS
        )
        return [{**row, "date": context.partition_date} for row in rows]
