import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.pinterest_ads import schemas
from interloper_assets.pinterest_ads.connection import PinterestAdsConnection

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": PinterestAdsConnection},
    tags=["Advertising"],
    icon="logos:pinterest",
)
class PinterestAds(il.Source):
    """Pinterest Ads advertising platform integration."""

    account_id: str = il.FetchField(
        endpoint="pinterest-ads/accounts",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="Pinterest Ads account",
    )

    # --- Entity assets (metadata) ---

    @il.asset(schema=schemas.AdAccounts, tags=["Entity"])
    def ad_accounts(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad account metadata including ownership and billing details."""
        return fake_data(schemas.AdAccounts)

    @il.asset(schema=schemas.Ads, tags=["Entity"])
    def ads(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad entity metadata including creative details and review status."""
        return fake_data(schemas.Ads)

    @il.asset(schema=schemas.AdGroups, tags=["Entity"])
    def ad_groups(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad group metadata including targeting, budget, and scheduling details."""
        return fake_data(schemas.AdGroups)

    @il.asset(schema=schemas.Campaigns, tags=["Entity"])
    def campaigns(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Campaign metadata including budget and scheduling details."""
        return fake_data(schemas.Campaigns)

    # --- Time-series report assets ---

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad-level performance report with engagement, cost, and conversion metrics."""
        return fake_data(schemas.AdsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Campaign-level performance report with engagement, cost, and conversion metrics."""
        return fake_data(schemas.CampaignsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.AdsConversionsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_conversions_stats(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Detailed conversion report with attribution breakdowns across channels and devices."""
        return fake_data(schemas.AdsConversionsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.VideosStatsByDevice,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def videos_stats_by_device(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Video ad performance report segmented by device type and placement."""
        return fake_data(schemas.VideosStatsByDevice, partition_column="date", partition_date=context.partition_date)
