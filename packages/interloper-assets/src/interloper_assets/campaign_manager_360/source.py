import logging

import interloper as il
import pandas as pd

from interloper_assets.campaign_manager_360.connection import CampaignManager360Connection
from interloper_assets.campaign_manager_360.schemas import (
    Ads,
    Campaigns,
    CustomAudiences,
    Reach,
)
from interloper_assets.fake import fake_data

logger = logging.getLogger(__name__)


@il.source(
    key="campaign_manager_360",
    resources={
        "connection": CampaignManager360Connection,
    },
    tags=["Advertising"],
    icon="icon:cm360",
)
class CampaignManager360(il.Source):
    """Campaign Manager 360 advertising platform integration."""

    profile_id: str = il.InputField(description="CM360 user profile ID")
    account_id: str = il.InputField(description="CM360 account ID")

    @il.asset(
        schema=Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns(
        self,
        context: il.ExecutionContext,
        connection: CampaignManager360Connection,
    ) -> pd.DataFrame:
        """Campaign-level performance metrics including impressions, clicks, conversions, and media costs."""
        return fake_data(Campaigns, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Ads,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads(
        self,
        context: il.ExecutionContext,
        connection: CampaignManager360Connection,
    ) -> pd.DataFrame:
        """Ad-level performance metrics including placement, creative, and media cost details."""
        return fake_data(Ads, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Reach,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def reach(
        self,
        context: il.ExecutionContext,
        connection: CampaignManager360Connection,
    ) -> pd.DataFrame:
        """Unique reach metrics including impression reach, viewable impression reach, and total reach."""
        return fake_data(Reach, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=CustomAudiences,
        tags=["Entity"],
    )
    def custom_audiences(
        self,
        connection: CampaignManager360Connection,
    ) -> pd.DataFrame:
        """Remarketing audience lists with population rules and lifecycle attributes."""
        return fake_data(CustomAudiences)
