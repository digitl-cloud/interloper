from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.tiktok_ads import constants
from interloper_assets.tiktok_ads.connection import TiktokAdsConnection
from interloper_assets.tiktok_ads.connector.schemas import (
    Adgroups,
    Ads,
    AdsStats,
    AdsStatsByAgeGender,
    AdsStatsByCountry,
    AdsStatsByPlatform,
    Advertisers,
    Campaigns,
    VideosStatsByPlatform,
)

# the reports read auction ads; reservation ads and coarser levels are other reports
_AUCTION_ADS = {"service_type": "AUCTION", "data_level": "AUCTION_AD"}
# the campaign, ad group and ad lists leave deleted objects out unless asked for every status, and a
# deleted object still carries spend in the reports
_EVERY_STATUS = {"filtering": {"primary_status": "STATUS_ALL"}}


# -- NORMALIZERS ---------------------------------------------------------------
class TiktokStatsNormalizer(DataFrameNormalizer):
    """Flatten TikTok integrated-report rows into a flat frame.

    Each row nests its requested dimensions under ``dimensions`` and its metrics
    under ``metrics``; merge both into a single flat record (dropping the parent
    prefix) before the base ``DataFrameNormalizer`` runs. A metric that does not
    apply to the row comes back as ``"-"`` and becomes NULL. ``stat_time_day``
    comes back as a midnight datetime string (``2026-01-01 00:00:00``); coerce it
    to a plain date so it lands on the ``date``-typed schema column.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        flat: list[dict[str, Any]] = []
        for row in records:
            merged = {}
            merged.update(row.get("dimensions") or {})
            merged.update({key: None if value == "-" else value for key, value in (row.get("metrics") or {}).items()})
            for key, value in row.items():
                if key not in ("dimensions", "metrics"):
                    merged[key] = value
            flat.append(merged)

        df = super().normalize(flat)
        if "stat_time_day" in df.columns:
            df["stat_time_day"] = pd.to_datetime(df["stat_time_day"], errors="coerce").dt.date
        return df


# Entity records carry list/dict fields (``ad_texts``, ``image_ids``,
# ``special_industries``, …) that the schemas type as strings; reconciliation
# JSON-encodes those nested values when casting to the declared ``str`` type.
_ENTITY_NORMALIZER = DataFrameNormalizer(drop_na_columns=True)
# the advertiser endpoint, unlike the campaign and ad ones, sends its creation time as seconds since the epoch
_ADVERTISER_NORMALIZER = DataFrameNormalizer(drop_na_columns=True, epoch_columns=["create_time"], epoch_unit="s")


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:tiktok-icon",
    normalizer=TiktokStatsNormalizer(),
)
class TiktokAds(il.Source):
    """TikTok Ads advertising platform integration."""

    connection: TiktokAdsConnection

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="advertiser_id",
        description="TikTok Ads advertiser account",
        discriminator=True,
    )

    # --- Time-series reports (TiktokStatsNormalizer from the source) ---

    @il.asset(schema=AdsStats, partitioning=il.TimePartitionConfig(column="stat_time_day"), tags=["Report"])
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance with basic metrics including spend, clicks, and conversions."""
        return await self.connection.api.report(
            context.partition_date,
            advertiser_id=self.advertiser_id,
            report_type="BASIC",
            dimensions=["ad_id", "stat_time_day"],
            metrics=constants.BASIC_METRICS,
            **_AUCTION_ADS,
        )

    @il.asset(schema=AdsStatsByCountry, partitioning=il.TimePartitionConfig(column="stat_time_day"), tags=["Report"])
    async def ads_stats_by_country(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad performance segmented by country."""
        return await self.connection.api.report(
            context.partition_date,
            advertiser_id=self.advertiser_id,
            report_type="AUDIENCE",
            dimensions=["ad_id", "country_code", "stat_time_day"],
            metrics=constants.AUDIENCE_METRICS,
            **_AUCTION_ADS,
        )

    @il.asset(schema=AdsStatsByAgeGender, partitioning=il.TimePartitionConfig(column="stat_time_day"), tags=["Report"])
    async def ads_stats_by_age_gender(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad performance segmented by age and gender demographics."""
        return await self.connection.api.report(
            context.partition_date,
            advertiser_id=self.advertiser_id,
            report_type="AUDIENCE",
            dimensions=["ad_id", "age", "gender", "stat_time_day"],
            metrics=constants.AUDIENCE_METRICS,
            **_AUCTION_ADS,
        )

    @il.asset(schema=AdsStatsByPlatform, partitioning=il.TimePartitionConfig(column="stat_time_day"), tags=["Report"])
    async def ads_stats_by_platform(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad performance segmented by platform."""
        return await self.connection.api.report(
            context.partition_date,
            advertiser_id=self.advertiser_id,
            report_type="AUDIENCE",
            dimensions=["ad_id", "platform", "stat_time_day"],
            metrics=constants.AUDIENCE_METRICS,
            **_AUCTION_ADS,
        )

    @il.asset(
        schema=VideosStatsByPlatform,
        partitioning=il.TimePartitionConfig(column="stat_time_day"),
        tags=["Report"],
    )
    async def videos_stats_by_platform(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Video ad performance segmented by platform."""
        return await self.connection.api.report(
            context.partition_date,
            advertiser_id=self.advertiser_id,
            report_type="AUDIENCE",
            dimensions=["ad_id", "platform", "stat_time_day"],
            metrics=constants.VIDEO_METRICS,
            **_AUCTION_ADS,
        )

    # --- Entity assets (_ENTITY_NORMALIZER) ---

    @il.asset(
        schema=Ads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ads in the advertiser account with their attributes."""
        rows = await self.connection.api.rows("/ad/get/", advertiser_id=self.advertiser_id, **_EVERY_STATUS)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaigns in the advertiser account with their attributes."""
        rows = await self.connection.api.rows("/campaign/get/", advertiser_id=self.advertiser_id, **_EVERY_STATUS)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Adgroups,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def adgroups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ad groups in the advertiser account with their schedule, budget, targeting and status."""
        rows = await self.connection.api.rows("/adgroup/get/", advertiser_id=self.advertiser_id, **_EVERY_STATUS)
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Advertisers,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ADVERTISER_NORMALIZER,
    )
    async def advertisers(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The advertiser account with its attributes."""
        data = await self.connection.api.get(
            "/advertiser/info/",
            advertiser_ids=[self.advertiser_id],
            fields=constants.ADVERTISER_FIELDS,
        )
        return [{**row, "date": context.partition_date} for row in data["list"]]
