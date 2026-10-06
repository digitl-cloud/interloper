"""Criteo star schema: facts and dimensions over every Criteo advertiser of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

PARTITIONING = il.TimePartitionConfig(column="date")


@il.source(tags=["Analytics"], icon="icon:criteo", maturity="alpha")
class CriteoStarSchema(il.Source):
    """Star schema over every Criteo advertiser (placeholder, not yet implemented).

    The fact table unions each advertiser's ad performance. Criteo exposes no
    entity snapshots, so the campaign dimension comes from the campaign
    reports and every other dimension from the facts.
    """

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "criteo.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, context: il.ExecutionContext, ads_stats: list[il.Upstream]) -> list[dict[str, Any]]:
        """Ad performance of every advertiser, one row per ad and day."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={"campaigns": il.Relation("asset", "criteo.campaigns_stats", many=True, optional=True)},
    )
    def dim_campaigns(
        self,
        context: il.ExecutionContext,
        campaigns: list[il.Upstream],
        fact_ad_performance: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per campaign, from the campaign reports, completed from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_ads(self, context: il.ExecutionContext, fact_ad_performance: il.Upstream) -> list[dict[str, Any]]:
        """One row per ad with its campaign, from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_accounts(self, context: il.ExecutionContext, fact_ad_performance: il.Upstream) -> list[dict[str, Any]]:
        """One row per advertiser with its name and currency, from the facts."""
        return []
