"""Amazon Ads star schema: facts and dimensions over every Amazon Ads profile of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

PARTITIONING = il.TimePartitionConfig(column="date")


@il.source(tags=["Analytics"], icon="icon:amazon", maturity="alpha")
class AmazonAdsStarSchema(il.Source):
    """Star schema over every Amazon Ads profile (placeholder, not yet implemented).

    The fact table unions the campaign performance of every ad product
    (Sponsored Products, Sponsored Brands, Sponsored Display) across
    profiles; the account dimension comes from the profile snapshots where
    they are collected, every other dimension from the facts.
    """

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Fact"],
        relations={
            "campaigns_stats": il.Relation(
                "asset",
                [
                    "amazon_ads.products_campaigns_stats",
                    "amazon_ads.brands_campaigns_stats",
                    "amazon_ads.display_campaigns_stats",
                ],
                many=True,
                optional=True,
            )
        },
    )
    def fact_campaign_performance(
        self,
        context: il.ExecutionContext,
        campaigns_stats: list[il.Upstream],
    ) -> list[dict[str, Any]]:
        """Campaign performance of every profile and ad product, one row per campaign and day."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_campaigns(
        self, context: il.ExecutionContext, fact_campaign_performance: il.Upstream
    ) -> list[dict[str, Any]]:
        """One row per campaign with its ad product, from the facts."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={"profiles": il.Relation("asset", "amazon_ads.profiles", many=True, optional=True)},
    )
    def dim_accounts(
        self,
        context: il.ExecutionContext,
        profiles: list[il.Upstream],
        fact_campaign_performance: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per profile with its marketplace and currency, from the profile snapshots and the facts."""
        return []
