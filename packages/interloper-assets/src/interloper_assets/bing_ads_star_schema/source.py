"""Bing Ads star schema: facts and dimensions over every Bing Ads account of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

PARTITIONING = il.TimePartitionConfig(column="date")


@il.source(tags=["Analytics"], icon="icon:bing", maturity="alpha")
class BingAdsStarSchema(il.Source):
    """Star schema over every Bing Ads account (placeholder, not yet implemented).

    The fact table unions each account's ad performance. Bing Ads collects no
    entity snapshots, so every dimension comes from the facts.
    """

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ads_stats(self, context: il.ExecutionContext, ads_stats: list[il.Upstream]) -> list[dict[str, Any]]:
        """Ad performance of every account, one row per ad and day."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_campaigns(self, context: il.ExecutionContext, fact_ads_stats: il.Upstream) -> list[dict[str, Any]]:
        """One row per campaign, from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_ads(self, context: il.ExecutionContext, fact_ads_stats: il.Upstream) -> list[dict[str, Any]]:
        """One row per ad with its ad group, from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_accounts(self, context: il.ExecutionContext, fact_ads_stats: il.Upstream) -> list[dict[str, Any]]:
        """One row per account with its name, from the facts."""
        return []
