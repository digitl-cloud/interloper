import logging

import interloper as il
import pandas as pd

from interloper_assets.campaign_matcher import schemas
from interloper_assets.facebook_ads.source import FacebookAds
from interloper_assets.fake import fake_data

logger = logging.getLogger(__name__)


@il.source(
    tags=["Analytics"],
)
class CampaignPerformanceAnalysis(il.Source):
    """Demo source. Defines a small DAG (a -> b,c,d -> e) with time partitioning."""

    @il.asset(
        schema=schemas.CampaignMatcher,
        tags=["Report"],
        requires={
            "facebook_ads_campaigns": FacebookAds.asset_def("campaigns_stats").qualified_key,
            # "google_ads_campaigns": GoogleAds.asset_def("campaigns_stats").qualified_key,
        },
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaign_matcher(
        self,
        context: il.ExecutionContext,
        facebook_ads_campaigns: pd.DataFrame | None = None,
        # google_ads_campaigns: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Group campaigns from multiple sources into a single basket."""
        return fake_data(schemas.CampaignMatcher, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.PerformanceAnalysis,
        tags=["Report"],
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def performance_analysis(
        self,
        context: il.ExecutionContext,
        campaign_matcher: pd.DataFrame,
    ) -> pd.DataFrame:
        """Analyze the performance of the campaigns."""
        return fake_data(schemas.PerformanceAnalysis, partition_column="date", partition_date=context.partition_date)
