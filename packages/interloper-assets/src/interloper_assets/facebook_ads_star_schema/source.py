"""Facebook Ads star schema: facts and dimensions over every Facebook Ads account of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

PARTITIONING = il.TimePartitionConfig(column="date")


@il.source(tags=["Analytics"], icon="logos:facebook", maturity="alpha")
class FacebookAdsStarSchema(il.Source):
    """Star schema over every Facebook Ads account (placeholder, not yet implemented).

    The fact table unions each account's ad performance; the dimensions are
    built from the account's entity snapshots where they are collected and
    from the facts otherwise, so every key a fact carries resolves.
    """

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ads_stats(self, context: il.ExecutionContext, ads_stats: list[il.Upstream]) -> list[dict[str, Any]]:
        """Ad performance of every account, one row per ad, day and placement breakdown."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation(
                "asset", ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"], many=True, optional=True
            )
        },
    )
    def dim_campaigns(
        self,
        context: il.ExecutionContext,
        campaigns: list[il.Upstream],
        fact_ads_stats: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per campaign, from the campaign snapshots or reports, completed from the facts."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={"ads": il.Relation("asset", "facebook_ads.ads", many=True, optional=True)},
    )
    def dim_ads(
        self,
        context: il.ExecutionContext,
        ads: list[il.Upstream],
        fact_ads_stats: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per ad with its ad set, from the ad snapshots, completed from the facts."""
        return []

    @il.asset(partitioning=PARTITIONING, tags=["Dimension"])
    def dim_accounts(self, context: il.ExecutionContext, fact_ads_stats: il.Upstream) -> list[dict[str, Any]]:
        """One row per ad account with its name and currency, from the facts."""
        return []
