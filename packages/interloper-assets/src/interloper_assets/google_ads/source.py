import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.google_ads.connection import GoogleAdsConnection
from interloper_assets.google_ads.schemas import AdsStats, CampaignsStats

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": GoogleAdsConnection},
    tags=["Advertising"],
    icon="logos:google-ads",
)
class GoogleAds(il.Source):
    """Google Ads advertising platform integration."""

    customer_id: str = il.FetchField(
        endpoint="google-ads/customers",
        depends_on="connection",
        label_key="name",
        value_key="customer_id",
        description="Google Ads customer ID (without hyphens)",
    )
    login_customer_id: str | None = il.InputField(
        default=None,
        description="Manager account customer ID (required if accessing through a manager account)",
    )

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def campaigns_stats(
        self,
        context: il.ExecutionContext,
        connection: GoogleAdsConnection,
    ) -> pd.DataFrame:
        """Campaign performance metrics including impressions, clicks, conversions, and cost."""
        return fake_data(CampaignsStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def ads_stats(
        self,
        context: il.ExecutionContext,
        connection: GoogleAdsConnection,
    ) -> pd.DataFrame:
        """Ad group ad performance metrics segmented by device, click type, and keyword."""
        return fake_data(AdsStats, partition_column="date", partition_date=context.partition_date)
