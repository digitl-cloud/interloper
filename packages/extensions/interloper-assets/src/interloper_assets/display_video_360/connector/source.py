from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection
from interloper_assets.display_video_360.connector import schemas

# both reports are standard reports; reach, Floodlight and YouTube reports are other types
_STANDARD_REPORT = {"type": "STANDARD", "options": {}}


# -- NORMALIZER ----------------------------------------------------------------
class DisplayVideo360Normalizer(DataFrameNormalizer):
    """Snake-case Bid Manager CSV report headers onto the schema columns.

    Report headers are human-readable ("Active View: % Viewable Impressions",
    "CM360 Placement ID"). The generic pass drops ``%`` outright, which would
    collide "% Viewable Impressions" with "Viewable Impressions", so spell it
    out as ``pct`` first, then defer to snake-casing with digit splitting
    (CM360 → cm_360). A no-op for the entity assets' camelCase JSON payloads.
    """

    def column_name(self, name: str) -> str:
        """Rewrite one vendor column name before the standard mapping.

        Args:
            name: The column name as the API returned it.

        Returns:
            The mapped column name.
        """
        return super().column_name(name.replace("%", "pct"))


# -- SOURCE --------------------------------------------------------------------
@il.source(
    key="display_video_360",
    tags=["Advertising"],
    icon="icon:dv360",
    # Partner objects nest config two levels deep (dataAccessConfig.sdfConfig.*);
    # digit splitting maps dv360ToCm... -> dv_360_to_cm_... onto the schema fields.
    normalizer=DisplayVideo360Normalizer(snake_case_digits=True, flatten_max_level=2),
    maturity="beta",
)
class DisplayVideo360(il.Source):
    """Display & Video 360 advertising platform integration."""

    connection: DisplayVideo360Connection

    partner_id: str = il.FetchField(
        label="Partner ID",
        description="DV360 partner",
        provider="connection.partners",
        label_key="name",
        value_key="partner_id",
        discriminator=True,
    )
    advertiser_id: str = il.InputField(
        default="",
        label="Advertiser ID",
        description=(
            "DV360 advertiser ID, narrows report assets to one advertiser and "
            "scopes audience assets by advertiser instead of partner"
        ),
    )
    audience_id: str = il.InputField(
        default="",
        label="Audience ID",
        description="Audience ID (only required for the custom_audiences asset)",
    )

    @property
    def _audience_scope(self) -> dict[str, str]:
        """Audience calls take a partner XOR advertiser scope; advertiser wins when set."""
        if self.advertiser_id:
            return {"advertiserId": self.advertiser_id}
        return {"partnerId": self.partner_id}

    @property
    def _report_filters(self) -> list[dict[str, str]]:
        """Report filters: always the partner, narrowed by advertiser when set."""
        filters = [{"type": "FILTER_PARTNER", "value": self.partner_id}]
        if self.advertiser_id:
            filters.append({"type": "FILTER_ADVERTISER", "value": self.advertiser_id})
        return filters

    # --- Reports (Bid Manager API) ---

    @il.asset(
        schema=schemas.LineItemsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def line_items_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Line-item performance, cost and fee metrics per day."""
        return self.connection.api.report(
            context.partition_date,
            title=f"line_items_stats_{context.partition_date.isoformat()}",
            groupBys=constants.LINE_ITEM_DIMENSIONS,
            filters=self._report_filters,
            metrics=constants.LINE_ITEM_METRICS,
            **_STANDARD_REPORT,
        )

    @il.asset(
        schema=schemas.LineItemsStatsByCountry,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def line_items_stats_by_country(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Line-item performance and cost metrics per day, broken down by country."""
        return self.connection.api.report(
            context.partition_date,
            title=f"line_items_stats_by_country_{context.partition_date.isoformat()}",
            groupBys=constants.LINE_ITEM_COUNTRY_DIMENSIONS,
            filters=self._report_filters,
            metrics=constants.LINE_ITEM_COUNTRY_METRICS,
            **_STANDARD_REPORT,
        )

    # --- Entities (Display & Video API) ---

    @il.asset(
        schema=schemas.Partners,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def partners(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """DV360 partners and their configuration."""
        rows = self.connection.api.rows("partners")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Audiences,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def audiences(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """First-party and partner audiences of the configured partner or advertiser."""
        rows = self.connection.api.rows("firstPartyAndPartnerAudiences", **self._audience_scope)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.CustomAudiences,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def custom_audiences(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Detail of the configured audience, including per-surface audience sizes.

        Needs the source's Audience ID, and fails without one.
        """
        if not self.audience_id:
            raise ValueError("The custom_audiences asset requires the source's audience_id to be set")
        audience = self.connection.api.get(
            "firstPartyAndPartnerAudiences", **self._audience_scope, firstPartyAndPartnerAudienceId=self.audience_id
        )
        return [{**audience, "date": context.partition_date}]
