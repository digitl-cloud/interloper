import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.tiktok_ads.connection import TiktokAdsConnection
from interloper_assets.tiktok_ads.schemas import (
    AdsByAgeGender,
    AdsByCountry,
    AdsByPlatform,
    AdsMetadata,
    AdsReport,
    AdvertisersMetadata,
    CampaignsMetadata,
    VideosByPlatform,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": TiktokAdsConnection},
    tags=["Advertising"],
    icon="logos:tiktok-icon",
)
class TiktokAds(il.Source):
    """TikTok Ads advertising platform integration."""

    advertiser_id: str = il.FetchField(
        endpoint="tiktok-ads/advertisers",
        depends_on="connection",
        label_key="name",
        value_key="advertiser_id",
        description="TikTok Ads advertiser account",
    )

    # --- Time-series reports ---

    @il.asset(
        schema=AdsReport,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads(self, context: il.ExecutionContext, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Ad-level performance with basic metrics including spend, clicks, and conversions."""
        return fake_data(AdsReport, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_by_country(self, context: il.ExecutionContext, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Ad performance segmented by country."""
        return fake_data(AdsByCountry, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdsByAgeGender,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_by_age_gender(self, context: il.ExecutionContext, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Ad performance segmented by age and gender demographics."""
        return fake_data(AdsByAgeGender, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdsByPlatform,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_by_platform(self, context: il.ExecutionContext, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Ad performance segmented by platform."""
        return fake_data(AdsByPlatform, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=VideosByPlatform,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def videos_by_platform(self, context: il.ExecutionContext, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Video ad performance segmented by platform."""
        return fake_data(VideosByPlatform, partition_column="date", partition_date=context.partition_date)

    # --- Entity (metadata) assets ---

    @il.asset(schema=AdsMetadata, tags=["Entity"])
    def ads_metadata(self, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Metadata for all ads in the advertiser account."""
        return fake_data(AdsMetadata)

    @il.asset(schema=CampaignsMetadata, tags=["Entity"])
    def campaigns_metadata(self, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Metadata for all campaigns in the advertiser account."""
        return fake_data(CampaignsMetadata)

    @il.asset(schema=AdvertisersMetadata, tags=["Entity"])
    def advertisers_metadata(self, connection: TiktokAdsConnection) -> pd.DataFrame:
        """Metadata for advertiser accounts."""
        return fake_data(AdvertisersMetadata)
