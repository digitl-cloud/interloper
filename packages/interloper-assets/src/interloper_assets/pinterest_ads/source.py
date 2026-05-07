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

    @il.asset(schema=schemas.AdsMetadata, tags=["Entity"])
    def ads_metadata(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad entity metadata including creative details and review status."""
        return fake_data(schemas.AdsMetadata)

    @il.asset(schema=schemas.AdGroupsMetadata, tags=["Entity"])
    def ad_groups_metadata(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad group metadata including targeting, budget, and scheduling details."""
        return fake_data(schemas.AdGroupsMetadata)

    @il.asset(schema=schemas.CampaignsMetadata, tags=["Entity"])
    def campaigns_metadata(self, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Campaign metadata including budget and scheduling details."""
        return fake_data(schemas.CampaignsMetadata)

    # --- Time-series report assets ---

    @il.asset(
        schema=schemas.Ads,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Ad-level performance report with engagement, cost, and conversion metrics."""
        return fake_data(schemas.Ads, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Campaign-level performance report with engagement, cost, and conversion metrics."""
        return fake_data(schemas.Campaigns, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.AdsConversions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_conversions(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Detailed conversion report with attribution breakdowns across channels and devices."""
        return fake_data(schemas.AdsConversions, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.VideosByDevice,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def videos_by_device(self, context: il.ExecutionContext, connection: PinterestAdsConnection) -> pd.DataFrame:
        """Video ad performance report segmented by device type and placement."""
        return fake_data(schemas.VideosByDevice, partition_column="date", partition_date=context.partition_date)
