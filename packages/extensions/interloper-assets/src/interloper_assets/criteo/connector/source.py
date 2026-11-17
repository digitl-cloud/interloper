from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.criteo import constants
from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.connector.schemas import Ads, AdSets, AdsStats, Advertisers, Campaigns, CampaignsStats

# Entity resources nest objects up to four levels deep (an ad set's targeting rules); lists are kept as JSON.
_ENTITY_NORMALIZER = DataFrameNormalizer(flatten_max_level=4, drop_na_columns=True)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:criteo",
    normalizer=DataFrameNormalizer(
        snake_case_digits=True,
        column_overrides={
            "OmnichannelRoasPc30d": "omni_channel_roas_pc_30d",
            "OmnichannelRevenuePc30d": "omni_channel_revenue_pc_30d",
            "OmnichannelSalesPc30d": "omni_channel_sales_pc_30d",
            "OmnichannelsalesClientAttribution": "omnichannel_sales_client_attribution",
        },
    ),
)
class Criteo(il.Source):
    """Criteo advertising platform integration."""

    connection: CriteoConnection

    advertiser_id: str = il.FetchField(
        provider="connection.advertisers",
        label_key="name",
        value_key="id",
        description="Criteo advertiser account",
        discriminator=True,
    )
    timezone: str = il.TimezoneField(
        default="UTC",
        description="Time zone the reports' days are in. Criteo does not state an advertiser's own, so set the one "
        "its account reports in.",
    )

    @il.asset(
        schema=AdsStats,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    async def ads_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Ad-level performance statistics with daily breakdowns, in euros and the source's time zone."""
        return await self.connection.api.report(
            context.partition_date,
            advertiserIds=self.advertiser_id,
            dimensions=["Day", "AdId", "AdsetId", "CampaignId"],
            metrics=constants.STATISTICS_METRICS,
            currency=constants.REPORT_CURRENCY,
            timezone=self.timezone,
        )

    @il.asset(
        schema=CampaignsStats,
        partitioning=il.TimePartitionConfig(column="day"),
        tags=["Report"],
    )
    async def campaigns_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Campaign-level performance statistics with daily breakdowns, in euros and the source's time zone."""
        return await self.connection.api.report(
            context.partition_date,
            advertiserIds=self.advertiser_id,
            dimensions=["Day", "CampaignId"],
            metrics=constants.STATISTICS_METRICS,
            currency=constants.REPORT_CURRENCY,
            timezone=self.timezone,
        )

    @il.asset(schema=Advertisers, partitioning=il.SnapshotPartitionConfig(column="date"), tags=["Entity"])
    async def advertisers(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """The source's advertiser with its name."""
        advertisers = await self.connection.api.get("/advertisers/me")
        rows = [row for row in advertisers if str(row["id"]) == self.advertiser_id]
        if not rows:
            raise RuntimeError(
                f"Criteo does not list advertiser {self.advertiser_id}: the connection no longer sees it"
            )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All campaigns of the advertiser with their goal, spend limit and budget automation."""
        rows = await self.connection.api.post(
            "/marketing-solutions/campaigns/search", filters={"advertiserIds": [self.advertiser_id]}
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=AdSets,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_sets(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ad sets of the advertiser with their schedule, status, budget, bidding and targeting."""
        rows = await self.connection.api.post(
            "/marketing-solutions/ad-sets/search", filters={"advertiserIds": [self.advertiser_id]}
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=Ads,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ads(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """All ads of the advertiser with their ad set, creative and dates."""
        rows = await self.connection.api.rows(f"/marketing-solutions/advertisers/{self.advertiser_id}/ads")
        return [{**row, "date": context.partition_date} for row in rows]
