
import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.fake import fake_data
from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.schemas import (
    FollowerDemographics,
    PageStats,
    ShareStats,
)

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": LinkedinOrganicConnection},
    tags=["Social Media"],
    normalizer=DataFrameNormalizer(flatten_max_level=3),
    icon="devicon:linkedin",
)
class LinkedinOrganic(il.Source):
    """LinkedIn Organization page organic analytics integration."""

    organization_id: str = il.FetchField(
        endpoint="linkedin-organic/organizations",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="LinkedIn Organization page ID",
    )

    @il.asset(
        schema=PageStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def page_stats(self, context: il.ExecutionContext, connection: LinkedinOrganicConnection) -> pd.DataFrame:
        """Daily organization page statistics including views and clicks across sections and devices."""
        return fake_data(PageStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=ShareStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def share_stats(self, context: il.ExecutionContext, connection: LinkedinOrganicConnection) -> pd.DataFrame:
        """Daily organic share statistics including impressions, clicks, and engagement."""
        return fake_data(ShareStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(schema=FollowerDemographics, tags=["Entity"])
    def follower_demographics(self, connection: LinkedinOrganicConnection) -> pd.DataFrame:
        """Follower counts broken down by association type, function, geo, industry, and seniority."""
        return fake_data(FollowerDemographics)
