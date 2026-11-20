from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.google_ads import constants
from interloper_assets.google_ads.connection import GoogleAdsConnection
from interloper_assets.google_ads.connector import schemas


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:google-ads",
    normalizer=DataFrameNormalizer(flatten_max_level=3, drop_na_columns=True),
    maturity="beta",
)
class GoogleAds(il.Source):
    """Google Ads advertising platform integration."""

    connection: GoogleAdsConnection

    customer_id: str = il.FetchField(
        provider="connection.customers",
        label_key="name",
        value_key="customer_id",
        description="Google Ads customer ID (without hyphens)",
        discriminator=True,
    )
    login_customer_id: str | None = il.InputField(
        default=None,
        description="Manager account customer ID (required if accessing through a manager account)",
    )

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="segments_date"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign performance metrics per day, with campaign settings and budget."""
        return await self.connection.api.report(
            self.customer_id,
            context.partition_date,
            resource="campaign",
            fields=constants.CAMPAIGN_FIELDS,
            login_customer_id=self.login_customer_id,
        )

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="segments_date"),
        tags=["Report"],
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad performance metrics per ad, day and device.

        Performance Max serves through asset groups rather than ads, so its traffic is not in this report.
        """
        return await self.connection.api.report(
            self.customer_id,
            context.partition_date,
            resource="ad_group_ad",
            fields=constants.AD_FIELDS,
            login_customer_id=self.login_customer_id,
        )
