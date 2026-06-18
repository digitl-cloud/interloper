import logging

import interloper as il
import pandas as pd

from interloper_assets.facebook_insights.connection import FacebookInsightsConnection
from interloper_assets.facebook_insights.schemas import PageStats, PostStats
from interloper_assets.fake import fake_data

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": FacebookInsightsConnection},
    tags=["Social Media"],
    icon="logos:facebook",
)
class FacebookInsights(il.Source):
    """Facebook Page and Post Insights integration."""

    page_id: str = il.FetchField(
        endpoint="facebook-insights/pages",
        depends_on="connection",
        label_key="name",
        value_key="id",
        description="Facebook Page to retrieve insights for",
    )

    @il.asset(
        schema=PageStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def page_stats(
        self,
        context: il.ExecutionContext,
        connection: FacebookInsightsConnection,
    ) -> pd.DataFrame:
        """Daily page-level engagement, impressions, reactions, and video view metrics."""
        return fake_data(PageStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=PostStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def post_stats(
        self,
        context: il.ExecutionContext,
        connection: FacebookInsightsConnection,
    ) -> pd.DataFrame:
        """Per-post engagement, clicks, reactions, and video metrics for published posts."""
        return fake_data(PostStats, partition_column="date", partition_date=context.partition_date)
