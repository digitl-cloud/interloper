from typing import Any

import interloper as il

from interloper_assets.facebook_ads import constants
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.connector import schemas
from interloper_assets.facebook_ads.connector.normalizer import FacebookActionsNormalizer

# Shared insights breakdowns for the ad/campaign/video reports.
_PLATFORM_BREAKDOWNS = ["publisher_platform", "platform_position", "impression_device"]
# by action type alone: the normalizer pivots each type into one column, and per-device rates and unique counts
# cannot be summed into one
_ACTION_BREAKDOWNS = ["action_type"]

# entity lists read live objects, Graph's default, in pages of 100 rather than 25: deleted ones cannot be asked for
# (error 100/1815001), and archived ones, nearly spendless, would multiply a rate-limited snapshot several times over;
# an archived object keeps the row read while it was live, or gets its dimension row from the reports
_SNAPSHOT_PAGE = {"limit": 100}


# Entity nodes nest objects (targeting, creative, business) that the source normalizer would
# flatten into open-ended columns, and flattening business.name would overwrite the vendor's own
# business_name; the entity assets keep every object whole as JSON under the vendor's name.
_ENTITY_NORMALIZER = FacebookActionsNormalizer()


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="logos:facebook",
    normalizer=FacebookActionsNormalizer(flatten_max_level=1),
)
class FacebookAds(il.Source):
    """Facebook Ads (Meta Marketing) advertising platform integration."""

    connection: FacebookAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="account_id",
        description="Facebook Ads account ID",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.CampaignsStats,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign-level performance insights with metrics, engagement, and cost breakdowns."""
        return self.connection.api.report(
            f"act_{self.account_id}/insights",
            context.partition_date,
            fields=constants.CAMPAIGNS_FIELDS,
            level="campaign",
            breakdowns=_PLATFORM_BREAKDOWNS,
            action_breakdowns=_ACTION_BREAKDOWNS,
        )

    @il.asset(
        schema=schemas.AdsStats,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance insights with platform, device, and action breakdowns."""
        return self.connection.api.report(
            f"act_{self.account_id}/insights",
            context.partition_date,
            fields=constants.ADS_INSIGHT_FIELDS,
            level="ad",
            breakdowns=_PLATFORM_BREAKDOWNS,
            action_breakdowns=_ACTION_BREAKDOWNS,
        )

    @il.asset(
        schema=schemas.AdsStatsByAgeGender,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_stats_by_age_gender(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance insights broken down by age and gender demographics."""
        return self.connection.api.report(
            f"act_{self.account_id}/insights",
            context.partition_date,
            fields=constants.GENDER_AGE_FIELDS,
            level="ad",
            breakdowns=["gender", "age"],
            action_breakdowns=_ACTION_BREAKDOWNS,
        )

    @il.asset(
        schema=schemas.AdsStatsByCountry,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def ads_stats_by_country(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance insights broken down by country and region."""
        return self.connection.api.report(
            f"act_{self.account_id}/insights",
            context.partition_date,
            fields=constants.GEO_FIELDS,
            level="ad",
            breakdowns=["country", "region"],
            action_breakdowns=_ACTION_BREAKDOWNS,
        )

    @il.asset(
        schema=schemas.VideosStats,
        partitioning=il.TimePartitionConfig(column="date_start"),
        tags=["Report"],
    )
    def videos_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Video ad performance with view retention, engagement, and conversion metrics."""
        return self.connection.api.report(
            f"act_{self.account_id}/insights",
            context.partition_date,
            fields=constants.VIDEOS_FIELDS,
            level="ad",
            breakdowns=_PLATFORM_BREAKDOWNS,
            action_breakdowns=_ACTION_BREAKDOWNS,
        )

    @il.asset(
        schema=schemas.CustomAudiences,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
    )
    def custom_audiences(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Custom audiences with approximate size bounds for the account."""
        rows = self.connection.api.rows(
            f"act_{self.account_id}/customaudiences", fields=constants.CUSTOM_AUDIENCES_FIELDS
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Ads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad entities including creative, status, bidding and targeting."""
        rows = self.connection.api.rows(
            f"act_{self.account_id}/ads", fields=constants.AD_ENTITY_FIELDS, **_SNAPSHOT_PAGE
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign entities including objective, budget, and status configuration."""
        rows = self.connection.api.rows(
            f"act_{self.account_id}/campaigns", fields=constants.CAMPAIGN_ENTITY_FIELDS, **_SNAPSHOT_PAGE
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.AdAccounts,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def ad_accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The ad account with its name, currency, timezone, status and billing settings."""
        account = self.connection.api.get(f"act_{self.account_id}", fields=constants.AD_ACCOUNT_ENTITY_FIELDS)
        return [{**account, "date": context.partition_date}]

    @il.asset(
        schema=schemas.Adsets,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    def adsets(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad set entities including schedule, budget, bidding, learning stage and targeting."""
        rows = self.connection.api.rows(
            f"act_{self.account_id}/adsets", fields=constants.ADSET_ENTITY_FIELDS, **_SNAPSHOT_PAGE
        )
        return [{**row, "date": context.partition_date} for row in rows]
