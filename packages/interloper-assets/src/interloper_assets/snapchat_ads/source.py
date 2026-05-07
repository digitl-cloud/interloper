
import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.schemas import (
    AdAccountMetadata,
    AdAccountsMetadata,
    Ads,
    AdsByCountry,
    AdsMetadata,
    AdSquadsMetadata,
    Campaigns,
    CampaignsMetadata,
    VideosByOs,
)

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": SnapchatAdsConnection},
    tags=["Advertising"],
    icon="mdi:snapchat",
)
class SnapchatAds(il.Source):
    """Snapchat Ads advertising platform integration."""

    account_id: str = il.FetchField(
        endpoint="snapchat-ads/ad-accounts",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="Snapchat Ads ad account",
    )

    # --- Time-series reports ---

    @il.asset(
        schema=Ads,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Ad-level performance with core, additional, and conversion metrics."""
        return fake_data(Ads, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Campaign-level performance with core, additional, and conversion metrics."""
        return fake_data(Campaigns, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_by_country(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Ad performance segmented by country with delivery and conversion metrics."""
        return fake_data(AdsByCountry, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=VideosByOs,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def videos_by_os(self, context: il.ExecutionContext, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Video ad performance segmented by operating system."""
        return fake_data(VideosByOs, partition_column="date", partition_date=context.partition_date)

    # --- Entity (metadata) assets ---

    @il.asset(schema=AdAccountMetadata, tags=["Entity"])
    def ad_account_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for a single ad account."""
        return fake_data(AdAccountMetadata)

    @il.asset(schema=AdAccountsMetadata, tags=["Entity"])
    def ad_accounts_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all ad accounts in the organization."""
        return fake_data(AdAccountsMetadata)

    @il.asset(schema=AdsMetadata, tags=["Entity"])
    def ads_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all ads in the ad account."""
        return fake_data(AdsMetadata)

    @il.asset(schema=AdSquadsMetadata, tags=["Entity"])
    def ad_squads_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all ad squads in the ad account."""
        return fake_data(AdSquadsMetadata)

    @il.asset(schema=CampaignsMetadata, tags=["Entity"])
    def campaigns_metadata(self, connection: SnapchatAdsConnection) -> pd.DataFrame:
        """Metadata for all campaigns in the ad account."""
        return fake_data(CampaignsMetadata)
