import logging

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.fake import fake_data
from interloper_assets.linkedin_ads import schemas
from interloper_assets.linkedin_ads.connection import LinkedinAdsConnection

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": LinkedinAdsConnection},
    tags=["Advertising"],
    normalizer=DataFrameNormalizer(flatten_max_level=1),
    icon="devicon:linkedin",
)
class LinkedinAds(il.Source):
    """LinkedIn Ads advertising platform integration."""

    account_id: str = il.FetchField(
        endpoint="linkedin-ads/accounts",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="LinkedIn Ads account",
    )

    @il.asset(
        schema=schemas.AdAccounts,
        tags=["Entity"],
    )
    def ad_accounts(self, connection: LinkedinAdsConnection) -> pd.DataFrame:
        """Ad account metadata including name, currency, and status."""
        return fake_data(schemas.AdAccounts)

    @il.asset(
        schema=schemas.AdCampaignGroups,
        tags=["Entity"],
    )
    def ad_campaign_groups(self, connection: LinkedinAdsConnection) -> pd.DataFrame:
        """Campaign group metadata including scheduling information."""
        return fake_data(schemas.AdCampaignGroups)

    @il.asset(
        schema=schemas.AdCampaigns,
        tags=["Entity"],
    )
    def ad_campaigns(self, connection: LinkedinAdsConnection) -> pd.DataFrame:
        """Campaign metadata including budget, objective, and scheduling details."""
        return fake_data(schemas.AdCampaigns)

    @il.asset(
        schema=schemas.Ads,
        partitioning=il.TimePartitionConfig(column="date_range_start"),
        tags=["Report"],
    )
    def ads(
        self,
        context: il.ExecutionContext,
        connection: LinkedinAdsConnection,
    ) -> pd.DataFrame:
        """Daily ad analytics with performance, engagement, and conversion metrics."""
        return fake_data(schemas.Ads, partition_column="date_range_start", partition_date=context.partition_date)
