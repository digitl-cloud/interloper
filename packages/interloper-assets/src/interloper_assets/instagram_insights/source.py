import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.instagram_insights.connection import InstagramInsightsConnection
from interloper_assets.instagram_insights.schemas import (
    AccountInsights,
    DemographicsByAgeGender,
    DemographicsByCity,
    DemographicsByCountry,
    Engagement,
    MediaInsights,
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
        schema=AccountInsights,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def account_insights(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Daily account-level follower count and reach metrics."""
        return fake_data(AccountInsights, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Engagement,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def engagement(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Daily engagement metrics including interactions, likes, comments, shares, and saves."""
        return fake_data(Engagement, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=DemographicsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def demographics_by_country(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Follower demographics broken down by country."""
        return fake_data(DemographicsByCountry, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=DemographicsByCity,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def demographics_by_city(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Follower demographics broken down by city."""
        return fake_data(DemographicsByCity, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=DemographicsByAgeGender,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def demographics_by_age_gender(
        self,
        context: il.ExecutionContext,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Follower demographics broken down by age and gender."""
        return fake_data(DemographicsByAgeGender, partition_column="date", partition_date=context.partition_date)

    @il.asset(schema=Profiles, tags=["Entity"])
    def profiles(
        self,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Account profile metadata including follower, following, and media counts."""
        return fake_data(Profiles)

    @il.asset(schema=MediaInsights, tags=["Entity"])
    def media_insights(
        self,
        connection: InstagramInsightsConnection,
    ) -> pd.DataFrame:
        """Media objects with metadata and engagement counts."""
        return fake_data(MediaInsights)
