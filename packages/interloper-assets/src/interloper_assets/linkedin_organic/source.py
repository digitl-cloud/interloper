
import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.fake import fake_data
from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.schemas import (
    FollowerStatistics,
    PageStatistics,
    ShareStatistics,
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
        schema=PageStatistics,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def page_statistics(self, context: il.ExecutionContext, connection: LinkedinOrganicConnection) -> pd.DataFrame:
        """Daily organization page statistics including views and clicks across sections and devices."""
        return fake_data(PageStatistics, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=ShareStatistics,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def share_statistics(self, context: il.ExecutionContext, connection: LinkedinOrganicConnection) -> pd.DataFrame:
        """Daily organic share statistics including impressions, clicks, and engagement."""
        return fake_data(ShareStatistics, partition_column="date", partition_date=context.partition_date)

    @il.asset(schema=FollowerStatistics, tags=["Entity"])
    def follower_statistics(self, connection: LinkedinOrganicConnection) -> pd.DataFrame:
        """Follower counts broken down by association type, function, geo, industry, and seniority."""
        return fake_data(FollowerStatistics)
