from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_console import constants
from interloper_assets.search_console.connection import SearchConsoleConnection
from interloper_assets.search_console.connector import schemas


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["SEO"],
    icon="devicon:google",
    normalizer=DataFrameNormalizer(),
    maturity="beta",
)
class SearchConsole(il.Source):
    """Google Search Console integration for search analytics data."""

    connection: SearchConsoleConnection

    site_url: str = il.FetchField(
        label="Site URL",
        description="Search Console property (e.g. https://example.com/ or sc-domain:example.com)",
        provider="connection.sites",
        label_key="name",
        value_key="site_url",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.PageStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def page_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Search performance per day, page, query, country, device and search type, aggregated by page."""
        return await self.connection.api.report(
            self.site_url,
            context.partition_date,
            dimensions=constants.PAGE_DIMENSIONS,
            search_types=constants.SEARCH_TYPES,
            aggregationType="BY_PAGE",
            dataState="final",
        )

    @il.asset(
        schema=schemas.SiteStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def site_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Search performance per day, query, country, device and search type, aggregated by property."""
        return await self.connection.api.report(
            self.site_url,
            context.partition_date,
            dimensions=constants.SITE_DIMENSIONS,
            search_types=constants.SEARCH_TYPES,
            aggregationType="BY_PROPERTY",
            dataState="final",
        )

    @il.asset(
        schema=schemas.SiteStatsByCountryDevice,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def site_stats_by_country_device(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Search performance per day, country, device and search type."""
        return await self.connection.api.report(
            self.site_url,
            context.partition_date,
            dimensions=constants.SITE_BY_COUNTRY_DEVICE_DIMENSIONS,
            search_types=constants.SEARCH_TYPES,
            aggregationType="AUTO",
            dataState="final",
        )

    @il.asset(
        schema=schemas.SiteStatsByCountryPage,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def site_stats_by_country_page(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Search performance per day, country, page and search type, Discover and Google News included."""
        return await self.connection.api.report(
            self.site_url,
            context.partition_date,
            dimensions=constants.SITE_BY_COUNTRY_PAGE_DIMENSIONS,
            search_types=constants.ALL_SEARCH_TYPES,
            aggregationType="AUTO",
            dataState="final",
        )

    @il.asset(
        schema=schemas.SearchAppearanceStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def search_appearance_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Search performance per search result feature and search type, Discover and Google News included."""
        rows = await self.connection.api.report(
            self.site_url,
            context.partition_date,
            dimensions=constants.SEARCH_APPEARANCE_DIMENSIONS,
            search_types=constants.ALL_SEARCH_TYPES,
            aggregationType="AUTO",
            dataState="final",
        )
        # Rows are not grouped by DATE, so they carry no day of their own.
        return [{**row, "date": context.partition_date} for row in rows]
