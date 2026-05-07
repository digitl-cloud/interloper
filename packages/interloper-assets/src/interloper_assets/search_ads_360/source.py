
import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.schemas import AdGroups, Ads, Campaigns, Conversions

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
        schema=Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Campaign performance metrics from Search Ads 360."""
        return fake_data(Campaigns, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdGroups,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ad_groups(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Ad group performance metrics from Search Ads 360."""
        return fake_data(AdGroups, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Ads,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Ad-level performance metrics from Search Ads 360."""
        return fake_data(Ads, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Conversions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def conversions(self, context: il.ExecutionContext, connection: SearchAds360Connection) -> pd.DataFrame:
        """Conversion action metrics from Search Ads 360."""
        return fake_data(Conversions, partition_column="date", partition_date=context.partition_date)
