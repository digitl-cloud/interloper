import interloper as il
import pandas as pd

from interloper_assets.facebook_ads import schemas
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.fake import fake_data

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": FacebookAdsConnection},
    tags=["Advertising"],
    icon="logos:facebook",
)
class FacebookAds(il.Source):
    """Facebook Ads (Meta Marketing) advertising platform integration."""

    account_id: str = il.FetchField(
        endpoint="facebook-ads/accounts",
        depends_on="connection",
        label_key="name",
        value_key="account_id",
        description="Facebook Ads account ID",
    )

    @il.asset(
        schema=schemas.CampaignsReport,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def campaigns(
        self,
        context: il.ExecutionContext,
        connection: FacebookAdsConnection,
    ) -> pd.DataFrame:
        """Campaign-level performance insights with metrics, engagement, and cost breakdowns."""
        return fake_data(schemas.CampaignsReport, partition_column="date_start", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.AdsReport,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads(
        self,
        context: il.ExecutionContext,
        connection: FacebookAdsConnection,
    ) -> pd.DataFrame:
        """Ad-level performance insights with platform, device, and action breakdowns."""
        return fake_data(schemas.AdsReport, partition_column="date_start", partition_date=context.partition_date)

    @il.asset(
        schema=schemas.AdsByAgeGenderReport,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_by_age_gender(
        self,
        context: il.ExecutionContext,
        connection: FacebookAdsConnection,
    ) -> pd.DataFrame:
        """Ad-level performance insights broken down by age and gender demographics."""
        return fake_data(
            schemas.AdsByAgeGenderReport, partition_column="date_start", partition_date=context.partition_date
        )

    @il.asset(
        schema=schemas.AdsByCountryReport,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_by_country(
        self,
        context: il.ExecutionContext,
        connection: FacebookAdsConnection,
    ) -> pd.DataFrame:
        """Ad-level performance insights broken down by country and region."""
        return fake_data(
            schemas.AdsByCountryReport, partition_column="date_start", partition_date=context.partition_date
        )

    @il.asset(
        schema=schemas.CustomAudiences,
        tags=["Entity"],
    )
    def custom_audiences(
        self,
        connection: FacebookAdsConnection,
    ) -> pd.DataFrame:
        """Custom audiences with approximate size bounds for the account."""
        return fake_data(schemas.CustomAudiences)

    @il.asset(
        schema=schemas.VideosReport,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def videos(
        self,
        context: il.ExecutionContext,
        connection: FacebookAdsConnection,
    ) -> pd.DataFrame:
        """Video ad performance with view retention, engagement, and conversion metrics."""
        return fake_data(schemas.VideosReport, partition_column="date_start", partition_date=context.partition_date)
