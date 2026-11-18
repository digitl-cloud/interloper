from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.snapchat_ads import constants
from interloper_assets.snapchat_ads.connection import SnapchatAdsConnection
from interloper_assets.snapchat_ads.connector.normalizer import SnapchatStatsNormalizer
from interloper_assets.snapchat_ads.connector.schemas import (
    AdAccounts,
    Ads,
    AdSquads,
    AdsStats,
    AdsStatsByCountry,
    Campaigns,
    CampaignsStats,
    VideosStatsByOs,
)

# Stats fields requested for the ad/campaign performance reports.
_REPORT_METRICS = constants.CONVERSION_METRICS + constants.CORE_METRICS + constants.ADDITIONAL_METRICS


# Entity records are nested dicts; flatten and drop all-null columns.
_ENTITY_NORMALIZER = DataFrameNormalizer(flatten_max_level=3, drop_na_columns=True)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="mdi:snapchat",
    normalizer=SnapchatStatsNormalizer(),
)
class SnapchatAds(il.Source):
    """Snapchat Ads advertising platform integration."""

    connection: SnapchatAdsConnection

    account_id: str = il.FetchField(
        provider="connection.ad_accounts",
        label_key="name",
        value_key="id",
        description="Snapchat Ads ad account",
        discriminator=True,
    )

    # --- Time-series reports (SnapchatStatsNormalizer from the source) ---

    @il.asset(schema=AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance with core, additional, and conversion metrics."""
        rows = await self.connection.api.report(
            f"/adaccounts/{self.account_id}/stats", context.partition_date, breakdown="ad", fields=_REPORT_METRICS
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=CampaignsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign-level performance with core, additional, and conversion metrics."""
        rows = await self.connection.api.report(
            f"/adaccounts/{self.account_id}/stats", context.partition_date, breakdown="campaign", fields=_REPORT_METRICS
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=AdsStatsByCountry, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def ads_stats_by_country(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad performance segmented by country with delivery and conversion metrics."""
        rows = await self.connection.api.report(
            f"/adaccounts/{self.account_id}/stats",
            context.partition_date,
            breakdown="ad",
            fields=constants.CONVERSION_METRICS + constants.DELIVERY_METRICS,
            report_dimension=["country"],
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=VideosStatsByOs, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def videos_stats_by_os(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Video ad performance segmented by operating system."""
        rows = await self.connection.api.report(
            f"/adaccounts/{self.account_id}/stats",
            context.partition_date,
            breakdown="ad",
            fields=constants.VIDEOS_METRICS,
            report_dimension=["os"],
        )
        return [{**row, "date": context.partition_date} for row in rows]

    # --- Entity assets (flattening normalizer) ---

    @il.asset(
        schema=AdAccounts,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The source's ad account with its attributes."""
        rows = await self.connection.api.rows(f"/adaccounts/{self.account_id}", "adaccounts", "adaccount")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Ads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ads in the ad account with their attributes."""
        rows = await self.connection.api.rows(f"/adaccounts/{self.account_id}/ads", "ads", "ad")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=AdSquads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_squads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ad squads in the ad account with their attributes."""
        rows = await self.connection.api.rows(f"/adaccounts/{self.account_id}/adsquads", "adsquads", "adsquad")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaigns in the ad account with their attributes."""
        rows = await self.connection.api.rows(f"/adaccounts/{self.account_id}/campaigns", "campaigns", "campaign")
        return [{**row, "date": context.partition_date} for row in rows]
