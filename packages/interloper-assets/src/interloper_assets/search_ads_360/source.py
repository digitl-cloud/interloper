
import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.schemas import AdGroupsStats, AdsStats, CampaignsStats, ConversionsStats

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    name="Search Ads 360",
    tags=["Advertising"],
    icon="devicon:google",
    resources={"connection": SearchAds360Connection},
)
class SearchAds360(il.Source):
    """Search Ads 360 advertising platform integration."""

    manager_customer_id: str = il.InputField(description="SA360 manager account customer ID")
    customer_client_id: str = il.InputField(description="SA360 customer client ID to report on")

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Campaign performance metrics from Search Ads 360."""
        return fake_data(CampaignsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ad_groups_stats(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Ad group performance metrics from Search Ads 360."""
        return fake_data(AdGroupsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Ad-level performance metrics from Search Ads 360."""
        return fake_data(AdsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=ConversionsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def conversions_stats(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Conversion action metrics from Search Ads 360."""
        return fake_data(ConversionsStats, partition_column="date", partition_date=context.partition_date)
