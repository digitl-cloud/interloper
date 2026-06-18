import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.search_console.connection import SearchConsoleConnection
from interloper_assets.search_console.schemas import (
    PageStats,
    SearchAppearanceStats,
    SiteStats,
    SiteStatsByCountryDevice,
    SiteStatsByCountryPage,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": SearchConsoleConnection},
    tags=["SEO"],
    icon="devicon:google",
)
class SearchConsole(il.Source):
    """Google Search Console integration for search analytics data."""

    site_url: str = il.InputField(description="Site URL (e.g. https://example.com/ or sc-domain:example.com)")

    @il.asset(
        schema=PageStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def page_stats(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Page-level search analytics with metrics per page URL, query, country, device, and search type."""
        return fake_data(PageStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SearchAppearanceStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def search_appearance_stats(
        self, context: il.ExecutionContext, connection: SearchConsoleConnection
    ) -> pd.DataFrame:
        """Search appearance analytics showing how your site appears in different search result types."""
        return fake_data(SearchAppearanceStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SiteStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_stats(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Site-level search analytics aggregated by property with query, country, and device breakdowns."""
        return fake_data(SiteStats, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SiteStatsByCountryDevice,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_stats_by_country_device(
        self, context: il.ExecutionContext, connection: SearchConsoleConnection
    ) -> pd.DataFrame:
        """Site search analytics segmented by country and device type."""
        return fake_data(SiteStatsByCountryDevice, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SiteStatsByCountryPage,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_stats_by_country_page(
        self, context: il.ExecutionContext, connection: SearchConsoleConnection
    ) -> pd.DataFrame:
        """Site search analytics segmented by country and page URL."""
        return fake_data(SiteStatsByCountryPage, partition_column="date", partition_date=context.partition_date)
