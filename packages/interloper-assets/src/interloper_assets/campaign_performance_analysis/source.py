import interloper as il
import pandas as pd

from interloper_assets.campaign_performance_analysis import schemas
from interloper_assets.facebook_ads.source import FacebookAds
from interloper_assets.fake import fake_data
from interloper_assets.google_ads.source import GoogleAds
from interloper_assets.pinterest_ads.source import PinterestAds
from interloper_assets.snapchat_ads.source import SnapchatAds

partitioning = il.TimePartitionConfig(column="date", allow_window=False)


@il.source(
    tags=["Analytics"],
)
class CampaignPerformanceAnalysis(il.Source):
    """Campaign performance analysis source."""

    @il.asset(
        schema=schemas.CampaignMatcher,
        partitioning=partitioning,
        requires={
            "facebook_ads_campaigns": FacebookAds.asset_def("campaigns").qualified_key,
            "google_ads_campaigns": GoogleAds.asset_def("campaigns").qualified_key,
            "snapchat_ads_campaigns": SnapchatAds.asset_def("campaigns").qualified_key,
            "pinterest_ads_campaigns": PinterestAds.asset_def("campaigns").qualified_key,
        },
    )
    def campaign_matcher(
        self,
        context: il.ExecutionContext,
        facebook_ads_campaigns: pd.DataFrame | None = None,
        google_ads_campaigns: pd.DataFrame | None = None,
        snapchat_ads_campaigns: pd.DataFrame | None = None,
        pinterest_ads_campaigns: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Campaign matcher."""
        return fake_data(schemas.CampaignMatcher, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.PerformanceAnalysis,
        partitioning=partitioning,
    )
    def performance_analysis(
        self,
        context: il.ExecutionContext,
        campaign_matcher: pd.DataFrame,
    ) -> pd.DataFrame:
        """Performance analysis."""
        return fake_data(schemas.PerformanceAnalysis, partition_column="date", partition_date=context.partition_date)
