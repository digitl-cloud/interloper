"""The Trade Desk star schema: facts and dimensions over every The Trade Desk partner of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

PARTITIONING = il.TimePartitionConfig(column="date")


@il.source(tags=["Analytics"], icon="icon:thetradedesk", maturity="alpha")
class TheTradeDeskStarSchema(il.Source):
    """Star schema over every The Trade Desk partner (placeholder, not yet implemented).

    The fact table unions each partner's ad group performance. The Trade Desk
    collects no entity snapshots, so every dimension comes from the facts.
    """

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Fact"],
        relations={"ad_groups_stats": il.Relation("asset", "the_trade_desk.ad_groups_stats", many=True, optional=True)},
    )
    def fact_ad_group_performance(
        self,
        context: il.ExecutionContext,
        ad_groups_stats: list[il.Upstream],
    ) -> list[dict[str, Any]]:
        """Ad group performance of every partner, one row per ad group and day."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_campaigns(
        self, context: il.ExecutionContext, fact_ad_group_performance: il.Upstream
    ) -> list[dict[str, Any]]:
        """One row per campaign, from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_ad_groups(
        self, context: il.ExecutionContext, fact_ad_group_performance: il.Upstream
    ) -> list[dict[str, Any]]:
        """One row per ad group with its campaign, from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_accounts(
        self, context: il.ExecutionContext, fact_ad_group_performance: il.Upstream
    ) -> list[dict[str, Any]]:
        """One row per advertiser with its partner, from the facts."""
        return []
