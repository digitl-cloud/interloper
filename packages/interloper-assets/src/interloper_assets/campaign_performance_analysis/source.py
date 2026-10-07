"""Campaign performance analysis: performance of every matched campaign across advertising platforms."""

from __future__ import annotations

from typing import Any

import interloper as il


@il.source(tags=["Analytics"], icon="carbon:chart-line-data", maturity="alpha")
class CampaignPerformanceAnalysis(il.Source):
    """Campaign performance across every advertising platform (placeholder, not yet implemented).

    Every star schema's performance fact feeds it, rolled up to the campaign
    and keyed on the campaign matcher's canonical campaign, so a campaign run
    on several platforms reads as one.
    """

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Fact"],
        relations={
            "performance": il.Relation(
                "asset",
                ["*.fact_ad_performance", "*.fact_ad_group_performance", "*.fact_campaign_performance"],
                many=True,
                optional=True,
            ),
            "matches": il.Relation("asset", "campaign_matcher.campaign_matches", optional=True),
        },
    )
    def campaign_performance(
        self,
        context: il.ExecutionContext,
        performance: list[il.Upstream],
        matches: il.Upstream | None = None,
    ) -> list[dict[str, Any]]:
        """Performance of every matched campaign per day, summed across the platforms it runs on."""
        return []
