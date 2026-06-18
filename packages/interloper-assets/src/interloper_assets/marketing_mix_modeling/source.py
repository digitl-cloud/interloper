import interloper as il
import pandas as pd

from interloper_assets.facebook_ads.source import FacebookAds
from interloper_assets.fake import fake_data
from interloper_assets.google_ads.source import GoogleAds
from interloper_assets.marketing_mix_modeling import schemas
from interloper_assets.pinterest_ads.source import PinterestAds
from interloper_assets.snapchat_ads.source import SnapchatAds

partitioning = il.TimePartitionConfig(column="date", allow_window=False)


@il.source(
    tags=["Analytics"],
)
class MarketingMixModeling(il.Source):
    """Marketing mix modeling source."""

    @il.asset(
        schema=schemas.Channels,
        partitioning=partitioning,
        requires={
            "facebook_ads_campaigns": FacebookAds.asset_def("campaigns_stats").qualified_key,
            "google_ads_campaigns": GoogleAds.asset_def("campaigns_stats").qualified_key,
            "snapchat_ads_campaigns": SnapchatAds.asset_def("campaigns_stats").qualified_key,
            "pinterest_ads_campaigns": PinterestAds.asset_def("campaigns_stats").qualified_key,
        },
    )
    def channels(
        self,
        context: il.ExecutionContext,
        facebook_ads_campaigns: pd.DataFrame | None = None,
        google_ads_campaigns: pd.DataFrame | None = None,
        snapchat_ads_campaigns: pd.DataFrame | None = None,
        pinterest_ads_campaigns: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Campaign matcher."""
        return fake_data(schemas.Channels, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.Spendings,
        partitioning=partitioning,
    )
    def spendings(
        self,
        context: il.ExecutionContext,
        channels: pd.DataFrame,
    ) -> pd.DataFrame:
        """Spendings."""
        return fake_data(schemas.Spendings, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.Model,
        partitioning=partitioning,
    )
    def model(
        self,
        context: il.ExecutionContext,
        channels: pd.DataFrame,
        spendings: pd.DataFrame,
    ) -> pd.DataFrame:
        """Model."""
        return fake_data(schemas.Model, partition_column="date", partition_date=context.partition_date)
