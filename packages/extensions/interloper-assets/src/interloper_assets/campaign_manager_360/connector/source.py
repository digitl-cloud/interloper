from typing import Any

import interloper as il

from interloper_assets.campaign_manager_360 import constants
from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection
from interloper_assets.campaign_manager_360.connector import schemas
from interloper_assets.campaign_manager_360.connector.normalizer import CampaignManager360Normalizer

_Record = dict[str, Any]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="campaign_manager_360",
    tags=["Advertising"],
    icon="icon:cm360",
    normalizer=CampaignManager360Normalizer(snake_case_digits=True, flatten_max_level=2),
    maturity="beta",
)
class CampaignManager360(il.Source):
    """Campaign Manager 360 advertising platform integration."""

    connection: CampaignManager360Connection

    profile_id: str = il.FetchField(
        label="Profile ID",
        description="CM360 user profile",
        provider="connection.profiles",
        label_key="name",
        value_key="profile_id",
    )
    account_id: str = il.FetchField(
        label="Account ID",
        description="CM360 account",
        provider="connection.profiles",
        label_key="account_name",
        value_key="account_id",
        discriminator=True,
    )
    advertiser_id: str = il.InputField(
        default="",
        label="Advertiser ID",
        description="CM360 advertiser ID (only required for the custom_audiences asset)",
    )

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Campaign performance metrics per day."""
        return await self.connection.api.report(
            self.profile_id,
            context.partition_date,
            account_id=self.account_id,
            report_type="STANDARD",
            dimensions=constants.CAMPAIGN_DIMENSIONS,
            metrics=constants.CAMPAIGN_METRICS,
        )

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Ad performance metrics per day at placement/creative grain."""
        return await self.connection.api.report(
            self.profile_id,
            context.partition_date,
            account_id=self.account_id,
            report_type="STANDARD",
            dimensions=constants.AD_DIMENSIONS,
            metrics=constants.AD_METRICS,
        )

    @il.asset(
        schema=schemas.ReachStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def reach_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Unique-reach metrics per campaign and country."""
        rows = await self.connection.api.report(
            self.profile_id,
            context.partition_date,
            account_id=self.account_id,
            report_type="REACH",
            dimensions=constants.UNIQUE_REACH_DIMENSIONS,
            metrics=constants.UNIQUE_REACH_METRICS,
        )
        # The REACH report has no date dimension; stamp the partition day.
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.CustomAudiences,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    async def custom_audiences(self, context: il.ExecutionContext) -> list[_Record]:
        """Remarketing lists (custom audiences) of the configured advertiser.

        Needs the source's Advertiser ID, and fails without one.
        """
        if not self.advertiser_id:
            raise ValueError("The custom_audiences asset requires the source's advertiser_id to be set")
        rows = await self.connection.api.rows(
            f"/userprofiles/{self.profile_id}/remarketingLists", "remarketingLists", advertiserId=self.advertiser_id
        )
        return [{**row, "date": context.partition_date} for row in rows]
