import datetime as dt
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.search_ads_360 import constants, schemas
from interloper_assets.search_ads_360.connection import SearchAds360Connection

_Row = dict[str, Any]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="search_ads_360",
    name="Search Ads 360",
    tags=["Advertising"],
    icon="devicon:google",
    normalizer=DataFrameNormalizer(flatten_max_level=3, drop_na_columns=True),
)
class SearchAds360(il.Source):
    """Search Ads 360 advertising platform integration."""

    connection: SearchAds360Connection

    manager_customer_id: str = il.InputField(description="SA360 manager account customer ID")
    customer_client_id: str = il.InputField(description="SA360 customer client ID to report on", discriminator=True)

    async def _search(
        self,
        customer_id: str,
        resource: str,
        fields: list[str],
        *,
        date: dt.date | None = None,
        login_customer_id: str | None = None,
    ) -> list[_Row]:
        """Run a GAQL query through ``searchAds360:search``, following ``nextPageToken``.

        Rows come back as the REST API sends them: camelCase keys nested per
        resource, int64 values as strings, enums by name. The normalizer flattens
        each row into its full resource path (``metrics_clicks``, ``segments_date``).
        The endpoint is a POST whose paging token travels in the body, so the
        query-param paginators do not apply.

        Args:
            customer_id: The customer the query runs against.
            resource: The GAQL ``FROM`` resource (``campaign``, ``customer_client``).
            fields: The GAQL ``SELECT`` field paths.
            date: The day to report on, applied as the ``segments.date`` filter;
                ``None`` queries without a date filter.
            login_customer_id: The manager account the access goes through, sent
                as the ``login-customer-id`` header; ``None`` sends no header.

        Returns:
            The result rows across every page.
        """
        query = f"SELECT {', '.join(fields)} FROM {resource}"
        if date is not None:
            query += f" WHERE segments.date BETWEEN '{date.isoformat()}' AND '{date.isoformat()}'"
        headers = {"login-customer-id": login_customer_id} if login_customer_id else {}
        body: dict[str, str] = {"query": query}
        rows: list[_Row] = []
        while True:
            response = await self.connection.client.post(
                f"/customers/{customer_id}/searchAds360:search", json=body, headers=headers
            )
            response.raise_for_status()
            page = response.json()
            rows.extend(page.get("results") or [])
            page_token = page.get("nextPageToken")
            if not page_token:
                return rows
            body = {**body, "pageToken": page_token}

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="segments_date"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[_Row]:
        """Campaign performance metrics per day."""
        return await self._search(
            self.customer_client_id,
            "campaign",
            constants.CAMPAIGN_FIELDS,
            date=context.partition_date,
            login_customer_id=self.manager_customer_id,
        )

    @il.asset(
        schema=schemas.CustomerClients,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Entity"],
    )
    async def customer_clients(self, context: il.ExecutionContext) -> list[_Row]:
        """All client accounts, direct and indirect, under the manager account."""
        rows = await self._search(self.manager_customer_id, "customer_client", constants.CUSTOMER_CLIENT_FIELDS)
        return [{**row, "date": context.partition_date} for row in rows]
