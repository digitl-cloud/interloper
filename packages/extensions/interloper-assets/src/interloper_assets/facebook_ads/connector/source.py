import re
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_ads import constants
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.connector import schemas

# Insight columns whose cells are lists of ``{action_type, value}``; each is
# pivoted into one column per action type (``actions`` -> ``actions_link_click``).
PIVOT_COLUMNS = [
    "action_values",
    "actions",
    "conversions",
    "cost_per_action_type",
    "cost_per_conversion",
    "cost_per_outbound_click",
    "cost_per_unique_action_type",
    "cost_per_unique_outbound_click",
    "outbound_clicks_ctr",
    "outbound_clicks",
    "unique_actions",
    "unique_outbound_clicks_ctr",
    "unique_outbound_clicks",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_play_actions",
    "website_ctr",
]

# Shared insights breakdowns for the ad/campaign/video reports.
_PLATFORM_BREAKDOWNS = ["publisher_platform", "platform_position", "impression_device"]
_ACTION_BREAKDOWNS = ["action_device", "action_type"]

# entity lists read live objects, Graph's default, in pages of 100 rather than 25: deleted ones cannot be asked for
# (error 100/1815001), and archived ones, nearly spendless, would multiply a rate-limited snapshot several times over;
# an archived object keeps the row read while it was live, or gets its dimension row from the reports
_SNAPSHOT_PAGE = {"limit": 100}


# -- HELPERS: flattening -------------------------------------------------------
def _sanitize(name: str) -> str:
    """Lower-case a column name and collapse non-alphanumeric runs to ``_``."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()


def _pivot_cell(column: str, cell: Any) -> dict[str, float]:
    """Pivot a list of ``{action_type, value}`` into ``{column_actiontype: value}``.

    Values are summed across the action-device breakdown so each action type
    yields a single column, mirroring the reference reporter.
    """
    out: dict[str, float] = {}
    if isinstance(cell, list):
        for entry in cell:
            action_type = entry.get("action_type")
            if action_type is None:
                continue
            value = pd.to_numeric(entry.get("value"), errors="coerce")
            if pd.isna(value):
                continue
            key = f"{column}_{action_type}"
            out[key] = out.get(key, 0.0) + float(value)
    return out


class FacebookActionsNormalizer(DataFrameNormalizer):
    """Pivot Facebook insight action-lists into per-action-type columns.

    Each ``actions``/``action_values``/… cell is a list of ``{action_type,
    value}``; pivot it into one ``<column>_<action_type>`` column (summed across
    the action-device breakdown), sanitize the names, coerce custom-conversion
    columns to float, then defer to the base ``DataFrameNormalizer`` to flatten
    nested entity dicts (``creative``) and snake-case the rest.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Reshape the vendor payload before the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        if df.empty:
            return df

        for column in PIVOT_COLUMNS:
            if column not in df.columns:
                continue
            cells = df[column].map(lambda cell, column=column: _pivot_cell(column, cell))
            pivoted = pd.DataFrame(list(cells), index=df.index)
            df = pd.concat([df.drop(columns=[column]), pivoted], axis=1)

        df.columns = [_sanitize(c) for c in df.columns]

        # Custom-conversion columns are not in the schema; coerce to float so
        # mixed int/NaN values don't surface as objects.
        for column in df.columns:
            if re.match(r".*_custom(_.+)?$", column):
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)

        return super().normalize(df)


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
            action_breakdown=_ACTION_BREAKDOWNS,
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
            action_breakdown=_ACTION_BREAKDOWNS,
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
            action_breakdown=_ACTION_BREAKDOWNS,
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
            action_breakdown=_ACTION_BREAKDOWNS,
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
            action_breakdown=_ACTION_BREAKDOWNS,
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
