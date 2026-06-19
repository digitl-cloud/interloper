import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.instagram_insights.connection import InstagramInsightsConnection
from interloper_assets.instagram_insights.schemas import (
    AccountStats,
    DemographicsStatsByAgeGender,
    DemographicsStatsByCity,
    DemographicsStatsByCountry,
    EngagementStats,
    Media,
    Profiles,
)


@il.source(
    resources={"connection": InstagramInsightsConnection},
    tags=["Social Media"],
    icon="skill-icons:instagram",
)
class InstagramInsights(il.Source):
    """Instagram Business and Creator account insights integration."""

    account_id: str = il.FetchField(
        endpoint="instagram-insights/accounts",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="Instagram Business or Creator account ID",
    )

    @il.asset(
        schema=AccountStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def account_stats(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Daily account-level follower count and reach metrics."""
        return fake_data(AccountStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=EngagementStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def engagement_stats(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Daily engagement metrics including interactions, likes, comments, shares, and saves."""
        return fake_data(EngagementStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=DemographicsStatsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def demographics_stats_by_country(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Follower demographics broken down by country."""
        return fake_data(DemographicsStatsByCountry, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=DemographicsStatsByCity,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def demographics_stats_by_city(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Follower demographics broken down by city."""
        return fake_data(DemographicsStatsByCity, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=DemographicsStatsByAgeGender,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def demographics_stats_by_age_gender(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Follower demographics broken down by age and gender."""
        return fake_data(DemographicsStatsByAgeGender, partition_column="date", partition_date=context.partition_date)

    @il.asset(schema=Profiles, tags=["Entity"])
    def profiles(
        self,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Account profile metadata including follower, following, and media counts."""
        return fake_data(Profiles)

    @il.asset(schema=Media, tags=["Entity"])
    def media(
        self,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Media objects with metadata and engagement counts."""
        return fake_data(Media)
