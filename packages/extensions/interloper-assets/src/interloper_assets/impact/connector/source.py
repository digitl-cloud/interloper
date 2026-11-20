from typing import Any

import interloper as il
from interloper_pandas import DataFrameNormalizer

from interloper_assets.impact.connection import ImpactConnection
from interloper_assets.impact.connector import schemas


@il.source(
    tags=["Affiliate"],
    icon="fluent:connector-24-filled",
    normalizer=DataFrameNormalizer(snake_case_digits=True, replace_empty_strings=True),
)
class Impact(il.Source):
    """Impact.com affiliate and partnership platform integration."""

    connection: ImpactConnection

    program_id: str = il.FetchField(
        provider="connection.programs",
        label_key="Name",
        value_key="Id",
        description="Impact program (campaign) ID",
        discriminator=True,
    )

    @il.asset(
        schema=schemas.Actions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def actions(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Conversion actions including revenue, commissions, and attribution data."""
        rows = await self.connection.api.rows(
            "/Actions",
            "Actions",
            context.partition_date,
            window=("ActionDateStart", "ActionDateEnd"),
            CampaignId=self.program_id,
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.ActionUpdates,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def action_updates(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Action updates tracking changes to conversion actions over time."""
        rows = await self.connection.api.rows(
            "/ActionUpdates", "ActionUpdates", context.partition_date, CampaignId=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.ActionInquiries,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def action_inquiries(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Action inquiries raised against conversion actions."""
        rows = await self.connection.api.rows(
            "/ActionInquiries", "ActionInquiries", context.partition_date, CampaignId=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.ActionsBySku,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def actions_by_sku(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Conversion actions broken down by product SKU."""
        rows = await self.connection.api.report(
            "adv_action_list_sku_pm_only", context.partition_date, SUBAID=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.Clicks,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def clicks(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Click events for the program."""
        rows = await self.connection.api.export(
            f"/Programs/{self.program_id}/ClickExport", "Clicks", Date=context.partition_date.isoformat()
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.PerformanceStats,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Daily performance metrics (clicks, actions, revenue)."""
        rows = await self.connection.api.report(
            "att_adv_performance_by_day_pm_only", context.partition_date, SUBAID=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.PerformanceStatsByAd,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_ad(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance metrics broken down by ad."""
        rows = await self.connection.api.report(
            "adv_performance_by_ad_pm_only", context.partition_date, SUBAID=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.PerformanceStatsByDomain,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_domain(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance metrics broken down by referring domain."""
        rows = await self.connection.api.report(
            "att_adv_performance_by_ref_domain_pm_only", context.partition_date, SUBAID=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.PerformanceStatsBySharedId,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_shared_id(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance metrics broken down by shared ID."""
        rows = await self.connection.api.report(
            "att_adv_performance_by_shared_id_pm_only", context.partition_date, SUBAID=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]

    @il.asset(
        schema=schemas.PerformanceStatsByIo,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    async def performance_stats_by_io(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        """Performance metrics broken down by insertion order."""
        rows = await self.connection.api.report(
            "att_adv_performance_by_IO", context.partition_date, SUBAID=self.program_id
        )
        return [{**row, "date": context.partition_date} for row in rows]
