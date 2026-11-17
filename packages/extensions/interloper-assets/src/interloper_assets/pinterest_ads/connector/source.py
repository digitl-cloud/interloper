from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.pinterest_ads import constants
from interloper_assets.pinterest_ads.connection import PinterestAdsConnection
from interloper_assets.pinterest_ads.connector import schemas

_Record = dict[str, Any]


# -- NORMALIZERS ---------------------------------------------------------------
# Report columns come back UPPER_SNAKE; the snake-caser splits "3SEC"/"15SEC" into "3_sec"/"15_sec".
_REPORT_NORMALIZER = DataFrameNormalizer(
    column_overrides={
        "TOTAL_VIDEO_3SEC_VIEWS": "total_video_3sec_views",
        "TOTAL_VIDEO_15SEC_UNIQUE_VIEWS": "total_video_15sec_unique_views",
        "VIDEO_3SEC_VIEWS_1": "video_3sec_views_1",
        "VIDEO_3SEC_VIEWS_2": "video_3sec_views_2",
        "VIDEO_15SEC_UNIQUE_VIEWS_1": "video_15sec_unique_views_1",
        "VIDEO_15SEC_UNIQUE_VIEWS_2": "video_15sec_unique_views_2",
    },
)

# Entities nest small objects (owner, targeting_spec, bid_options, …), mirrored one level deep as
# <parent>_<key> columns; lifecycle and schedule times arrive as seconds since the epoch.
_ENTITY_NORMALIZER = DataFrameNormalizer(
    flatten_max_level=1,
    epoch_columns=["created_time", "updated_time", "start_time", "end_time"],
    epoch_unit="s",
)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:pinterest",
    normalizer=_REPORT_NORMALIZER,
    maturity="beta",
)
class PinterestAds(il.Source):
    """Pinterest Ads advertising platform integration."""

    connection: PinterestAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="Pinterest Ads account",
        discriminator=True,
    )

    # -- REPORTS ---------------------------------------------------------------
    @il.asset(schema=schemas.AdsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def ads_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily ad performance: delivery, engagement, spend and conversion totals per ad."""
        return await self.connection.api.report(
            self.account_id,
            context.partition_date,
            granularity="DAY",
            level="PIN_PROMOTION",
            columns=[*constants.AD_METRICS, "AD_ID"],
        )

    @il.asset(schema=schemas.CampaignsStats, partitioning=il.TimePartitionConfig(column="date"), tags=["Report"])
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily campaign performance: delivery, engagement, spend and conversion totals per campaign."""
        return await self.connection.api.report(
            self.account_id,
            context.partition_date,
            granularity="DAY",
            level="CAMPAIGN",
            columns=constants.CAMPAIGN_METRICS,
        )

    @il.asset(
        schema=schemas.AdsConversionsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def ads_conversions_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily ad performance with the full conversion breakdown by event, attribution, channel and device."""
        return await self.connection.api.report(
            self.account_id,
            context.partition_date,
            granularity="DAY",
            level="PIN_PROMOTION",
            columns=constants.ADS_CONVERSIONS_METRICS,
        )

    @il.asset(
        schema=schemas.VideosStatsByTargeting,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def videos_stats_by_targeting(self, context: il.ExecutionContext) -> list[_Record]:
        """Daily video ad performance per ad, broken down by app type and placement."""
        return await self.connection.api.report(
            self.account_id,
            context.partition_date,
            granularity="DAY",
            level="PIN_PROMOTION_TARGETING",
            columns=constants.VIDEOS_METRICS,
            targeting_types=constants.VIDEO_TARGETING_TYPES,
        )

    # -- ENTITIES --------------------------------------------------------------
    @il.asset(
        schema=schemas.AdAccounts,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_accounts(self, context: il.ExecutionContext) -> list[_Record]:
        """The ad account with its owner, country, currency and permissions."""
        account = await self.connection.api.get(f"/ad_accounts/{self.account_id}")
        return [{**account, "date": context.partition_date}]

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[_Record]:
        """All active and paused campaigns in the ad account with their attributes."""
        rows = await self.connection.api.rows(f"/ad_accounts/{self.account_id}/campaigns")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.AdGroups,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_groups(self, context: il.ExecutionContext) -> list[_Record]:
        """All active and paused ad groups in the ad account with their attributes."""
        rows = await self.connection.api.rows(f"/ad_accounts/{self.account_id}/ad_groups")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Ads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ads(self, context: il.ExecutionContext) -> list[_Record]:
        """All active and paused ads in the ad account with their attributes."""
        rows = await self.connection.api.rows(f"/ad_accounts/{self.account_id}/ads")
        return [{**row, "date": context.partition_date} for row in rows]
