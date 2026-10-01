import datetime as dt
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_console import constants, schemas
from interloper_assets.search_console.connection import SearchConsoleConnection

_Record = dict[str, Any]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["SEO"],
    icon="devicon:google",
    normalizer=DataFrameNormalizer(),
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

    def _search_analytics(
        self,
        date: dt.date,
        *,
        dimensions: list[str],
        search_types: list[str],
        aggregation_type: str = "AUTO",
    ) -> list[_Record]:
        """Query Search Analytics for one day, once per search type, paging through every row.

        Shared by every asset: each is one Search Analytics query differing only in
        dimensions, search types and aggregation. A row carries its dimension values
        as a positional ``keys`` list, aligned to the requested ``dimensions``; each
        value is laid onto a column named after its dimension (``SEARCH_APPEARANCE``
        becomes ``search_appearance``). The search type is a request parameter the row
        does not echo, so it is stamped as ``search_type`` to keep the per-type rows
        of one table apart. The service is built once per call because assets run
        concurrently in threads and its HTTP transport is not thread-safe.

        Args:
            date: The day to query; both ends of the request's date range.
            dimensions: The Search Analytics dimensions to group by, in key order.
            search_types: The search types to query, one request series per type.
            aggregation_type: How the API aggregates the data (``AUTO``,
                ``BY_PROPERTY`` or ``BY_PAGE``). Defaults to ``AUTO``.

        Returns:
            The rows of every search type, one record per row with one column per
            dimension, the metrics as returned, and ``search_type``.
        """
        service = self.connection.client()
        records: list[_Record] = []
        for search_type in search_types:
            body = {
                "startDate": date.isoformat(),
                "endDate": date.isoformat(),
                "dimensions": dimensions,
                "type": search_type,
                "aggregationType": aggregation_type,
                "dataState": "final",
                "rowLimit": constants.ROW_LIMIT,
                "startRow": 0,
            }
            while True:
                response = service.searchanalytics().query(siteUrl=self.site_url, body=body).execute()
                rows = response.get("rows") or []
                for row in rows:
                    record = {dimension.lower(): key for dimension, key in zip(dimensions, row.get("keys") or [])}
                    record.update({name: value for name, value in row.items() if name != "keys"})
                    record["search_type"] = search_type.lower()
                    records.append(record)
                if len(rows) < constants.ROW_LIMIT:
                    break
                body["startRow"] += constants.ROW_LIMIT
        return records

    @il.asset(
        schema=schemas.PageStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def page_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Search performance per day, page, query, country, device and search type, aggregated by page."""
        return self._search_analytics(
            context.partition_date,
            dimensions=constants.PAGE_DIMENSIONS,
            search_types=constants.SEARCH_TYPES,
            aggregation_type="BY_PAGE",
        )

    @il.asset(
        schema=schemas.SiteStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Search performance per day, query, country, device and search type, aggregated by property."""
        return self._search_analytics(
            context.partition_date,
            dimensions=constants.SITE_DIMENSIONS,
            search_types=constants.SEARCH_TYPES,
            aggregation_type="BY_PROPERTY",
        )

    @il.asset(
        schema=schemas.SiteStatsByCountryDevice,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_stats_by_country_device(self, context: il.ExecutionContext) -> list[_Record]:
        """Search performance per day, country, device and search type."""
        return self._search_analytics(
            context.partition_date,
            dimensions=constants.SITE_BY_COUNTRY_DEVICE_DIMENSIONS,
            search_types=constants.SEARCH_TYPES,
        )

    @il.asset(
        schema=schemas.SiteStatsByCountryPage,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def site_stats_by_country_page(self, context: il.ExecutionContext) -> list[_Record]:
        """Search performance per day, country, page and search type, Discover and Google News included."""
        return self._search_analytics(
            context.partition_date,
            dimensions=constants.SITE_BY_COUNTRY_PAGE_DIMENSIONS,
            search_types=constants.ALL_SEARCH_TYPES,
        )

    @il.asset(
        schema=schemas.SearchAppearanceStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def search_appearance_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Search performance per search result feature and search type, Discover and Google News included."""
        rows = self._search_analytics(
            context.partition_date,
            dimensions=constants.SEARCH_APPEARANCE_DIMENSIONS,
            search_types=constants.ALL_SEARCH_TYPES,
        )
        # Rows are not grouped by DATE, so they carry no day of their own.
        return [{**row, "date": context.partition_date} for row in rows]
