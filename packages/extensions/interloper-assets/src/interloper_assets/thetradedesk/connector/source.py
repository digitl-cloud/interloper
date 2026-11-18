from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.thetradedesk import constants
from interloper_assets.thetradedesk.connection import TheTradeDeskConnection
from interloper_assets.thetradedesk.connector import schemas
from interloper_assets.thetradedesk.connector.normalizer import TheTradeDeskNormalizer

_Record = dict[str, Any]

# reports arrive in the international format TheTradeDeskNormalizer reads, every day a UTC day
_INTERNATIONAL_UTC = {
    "ReportDateFormat": "International",
    "ReportNumericFormat": "International",
    "TimeZone": constants.REPORT_TIMEZONE,
}


# GraphQL nodes nest their parent as an object (``campaign { id }``); one level
# of flattening gives the parent's id its own column, ``campaign_id``.
_ENTITY_NORMALIZER = DataFrameNormalizer(flatten_max_level=1)


# -- SOURCE --------------------------------------------------------------------
@il.source(
    tags=["Advertising"],
    icon="icon:thetradedesk",
    normalizer=TheTradeDeskNormalizer(replace_empty_strings=True),
)
class TheTradeDesk(il.Source):
    """The Trade Desk programmatic advertising platform integration."""

    connection: TheTradeDeskConnection

    partner_id: str = il.FetchField(
        label="Partner ID",
        description="The Trade Desk partner",
        provider="connection.partners",
        label_key="name",
        value_key="partner_id",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.AdGroupsStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def ad_groups_stats(self, context: il.ExecutionContext) -> list[_Record]:
        """Ad-group performance metrics per day, including video-player and conversion metrics."""
        return await self.connection.api.report(
            self.partner_id, constants.ADGROUPS_REPORT_TEMPLATE_ID, context.partition_date, **_INTERNATIONAL_UTC
        )

    @il.asset(
        schema=schemas.Advertisers,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def advertisers(self, context: il.ExecutionContext) -> list[_Record]:
        """The partner's advertisers with their currency, category, attribution windows and vetting status."""
        rows = await self.connection.api.nodes(
            constants.ADVERTISERS_QUERY, ("partner", "advertisers"), partnerId=self.partner_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def campaigns(self, context: il.ExecutionContext) -> list[_Record]:
        """The partner's campaigns with their advertiser, status, schedule (UTC instants), budget, pacing and goals."""
        rows = await self.connection.api.advertiser_nodes(self.partner_id, constants.CAMPAIGNS_QUERY, "campaigns")
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.AdGroups,
        partitioning=il.SnapshotPartitionConfig(column="date"),
        tags=["Entity"],
        normalizer=_ENTITY_NORMALIZER,
    )
    async def ad_groups(self, context: il.ExecutionContext) -> list[_Record]:
        """The partner's ad groups with their campaign, status, channel and budget; template ad groups excluded."""
        rows = await self.connection.api.advertiser_nodes(self.partner_id, constants.AD_GROUPS_QUERY, "adGroups")
        return [{**row, "date": context.partition_date} for row in rows]
