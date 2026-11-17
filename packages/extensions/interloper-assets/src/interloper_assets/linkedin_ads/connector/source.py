import datetime as dt
from typing import Any

import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer

from interloper_assets.linkedin_ads import constants
from interloper_assets.linkedin_ads.connection import LinkedinAdsConnection
from interloper_assets.linkedin_ads.connector.schemas import AdAccounts, AdsStats, CampaignGroups, Campaigns


# -- NORMALIZERS ---------------------------------------------------------------
class LinkedinAdsStatsNormalizer(DataFrameNormalizer):
    """Type the ``dateRange`` of ad analytics rows as days.

    Each row carries its day as ``{"start": {"year", "month", "day"}, "end": {...}}``.
    Both bounds become ``dt.date`` values before flattening, so they land on
    ``date_range_start`` / ``date_range_end`` under the vendor's own names.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Convert the ``dateRange`` bounds, then run the standard normalization.

        Args:
            data: The rows as the API returned them.

        Returns:
            The normalized frame.
        """
        records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
        rows = []
        for record in records:
            date_range = record.get("dateRange") or {}
            days = {bound: dt.date(**value) for bound, value in date_range.items() if isinstance(value, dict)}
            rows.append({**record, "dateRange": days} if days else record)
        return super().normalize(rows)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    normalizer=DataFrameNormalizer(
        flatten_max_level=1,
        # LinkedIn sends runSchedule instants as milliseconds since the epoch
        epoch_columns=["run_schedule_start", "run_schedule_end"],
        epoch_unit="ms",
    ),
    icon="devicon:linkedin",
    maturity="beta",
)
class LinkedinAds(il.Source):
    """LinkedIn Ads advertising platform integration."""

    connection: LinkedinAdsConnection

    account_id: str = il.FetchField(
        provider="connection.accounts",
        label_key="name",
        value_key="id",
        description="LinkedIn Ads account",
        discriminator=True,
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="date_range_start"),
        tags=["Report"],
        normalizer=LinkedinAdsStatsNormalizer(flatten_max_level=1),
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily cost, delivery, engagement and conversions of each sponsored share per campaign."""
        return await self.connection.api.report(
            context.partition_date,
            q="statistics",
            pivots=constants.ANALYTICS_PIVOTS,
            timeGranularity={"value": "DAILY"},
            accounts=[f"urn:li:sponsoredAccount:{self.account_id}"],
            fields=["pivotValues", "dateRange", *constants.ANALYTICS_FIELDS],
        )

    @il.asset(schema=AdAccounts, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def ad_accounts(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The ad account with its name, currency and status."""
        account = await self.connection.api.get(f"/adAccounts/{self.account_id}", fields=constants.ACCOUNT_FIELDS)
        return [{**account, "date": context.partition_date}]

    @il.asset(schema=CampaignGroups, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def campaign_groups(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaign groups of the ad account with their status and run schedule."""
        rows = await self.connection.api.rows(
            f"/adAccounts/{self.account_id}/adCampaignGroups", q="search", fields=constants.CAMPAIGN_GROUP_FIELDS
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(schema=Campaigns, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaigns of the ad account with their type, objective, budget and run schedule."""
        rows = await self.connection.api.rows(
            f"/adAccounts/{self.account_id}/adCampaigns", q="search", fields=constants.CAMPAIGN_FIELDS
        )
        return [{**row, "date": context.partition_date} for row in rows]
