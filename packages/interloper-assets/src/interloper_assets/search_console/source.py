import logging

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.search_console.connection import SearchConsoleConnection
from interloper_assets.search_console.schemas import (
    Page,
    SearchAppearance,
    Site,
    SiteByCountryDevice,
    SiteByCountryPage,
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
        schema=Page,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def page(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Page-level search analytics with metrics per page URL, query, country, device, and search type."""
        return fake_data(Page, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SearchAppearance,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def search_appearance(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Search appearance analytics showing how your site appears in different search result types."""
        return fake_data(SearchAppearance, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=Site,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Site-level search analytics aggregated by property with query, country, and device breakdowns."""
        return fake_data(Site, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SiteByCountryDevice,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_by_country_device(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Site search analytics segmented by country and device type."""
        return fake_data(SiteByCountryDevice, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=SiteByCountryPage,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_by_country_page(self, context: il.ExecutionContext, connection: SearchConsoleConnection) -> pd.DataFrame:
        """Site search analytics segmented by country and page URL."""
        return fake_data(SiteByCountryPage, partition_column="date", partition_date=context.partition_date)
