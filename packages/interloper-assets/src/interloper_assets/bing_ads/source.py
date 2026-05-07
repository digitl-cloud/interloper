
import interloper as il
import pandas as pd

from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.schemas import AdPerformance
from interloper_assets.fake import fake_data


@il.source(
    resources={"connection": BingAdsConnection},
    tags=["Advertising"],
    icon="icon:bing",
)
class BingAds(il.Source):
    """Bing Ads (Microsoft Advertising) platform integration."""

    @il.asset(
        schema=AdPerformance,
        partitioning=il.TimePartitionConfig(column="time_period"),
        tags=["Report"],
    )
    def ad_performance(self, context: il.ExecutionContext, connection: BingAdsConnection) -> pd.DataFrame:
        """Ad performance report with impressions, clicks, conversions, and revenue metrics."""
        return fake_data(AdPerformance, partition_column="time_period", partition_date=context.partition_date)
