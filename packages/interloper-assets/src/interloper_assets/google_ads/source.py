import datetime as dt
from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.google_ads import constants, schemas
from interloper_assets.google_ads.connection import GoogleAdsConnection

_Row = dict[str, Any]


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:google-ads",
    normalizer=DataFrameNormalizer(flatten_max_level=3, drop_na_columns=True),
)
class GoogleAds(il.Source):
    """Google Ads advertising platform integration."""

    connection: GoogleAdsConnection

    customer_id: str = il.FetchField(
        provider="connection.customers",
        label_key="name",
        value_key="customer_id",
        description="Google Ads customer ID (without hyphens)",
        discriminator=True,
    )
    login_customer_id: str | None = il.InputField(
        default=None,
        description="Manager account customer ID (required if accessing through a manager account)",
    )

    def _search_stream(self, resource: str, fields: list[str], date: dt.date) -> list[_Row]:
        """Run a one-day GAQL query against the customer and return its rows in REST JSON shape.

        The SDK yields proto-plus rows. ``MessageToDict`` over their protobuf form
        gives exactly what the REST API sends: camelCase keys nested per resource,
        int64 values as strings, enums by name, unset fields omitted, and ``type``
        where proto-plus would say ``type_``. The normalizer then flattens each row
        into its full resource path (``metrics_clicks``, ``ad_group_ad_ad_id``).

        Args:
            resource: The GAQL ``FROM`` resource (``campaign``, ``ad_group_ad``).
            fields: The GAQL ``SELECT`` field paths.
            date: The day to report on, applied as the ``segments.date`` filter.

        Returns:
            The result rows across every streamed batch.
        """
        from google.protobuf import json_format

        query = (
            f"SELECT {', '.join(fields)} FROM {resource} "
            f"WHERE segments.date BETWEEN '{date.isoformat()}' AND '{date.isoformat()}'"
        )
        metadata = [("login-customer-id", self.login_customer_id)] if self.login_customer_id else []
        service = self.connection.client.get_service("GoogleAdsService")
        stream = service.search_stream(customer_id=self.customer_id, query=query, metadata=metadata)
        return [json_format.MessageToDict(type(row).pb(row)) for batch in stream for row in batch.results]

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="segments_date"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext) -> list[_Row]:
        """Campaign performance metrics per day, with campaign settings and budget."""
        return self._search_stream("campaign", constants.CAMPAIGN_FIELDS, context.partition_date)

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="segments_date"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext) -> list[_Row]:
        """Ad performance metrics per day, segmented by device, click type and keyword."""
        return self._search_stream("ad_group_ad", constants.AD_FIELDS, context.partition_date)
