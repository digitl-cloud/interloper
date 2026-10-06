"""TikTok Ads star schema: facts and dimensions over every TikTok Ads advertiser of an organisation."""

from __future__ import annotations

from typing import Any

import interloper as il

PARTITIONING = il.TimePartitionConfig(column="date")


@il.source(tags=["Analytics"], icon="logos:tiktok-icon", maturity="alpha")
class TiktokAdsStarSchema(il.Source):
    """Star schema over every TikTok Ads advertiser (placeholder, not yet implemented).

    The fact table unions each advertiser's ad performance; the dimensions are
    built from the advertiser's entity snapshots where they are collected and
    from the facts otherwise, so every key a fact carries resolves.
    """

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "tiktok_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ads_stats(self, context: il.ExecutionContext, ads_stats: list[il.Upstream]) -> list[dict[str, Any]]:
        """Ad performance of every advertiser, one row per ad and day."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={"campaigns": il.Relation("asset", "tiktok_ads.campaigns", many=True, optional=True)},
    )
    def dim_campaigns(
        self,
        context: il.ExecutionContext,
        campaigns: list[il.Upstream],
        fact_ads_stats: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per campaign, from the campaign snapshots, completed from the facts."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={"ads": il.Relation("asset", "tiktok_ads.ads", many=True, optional=True)},
    )
    def dim_ads(
        self,
        context: il.ExecutionContext,
        ads: list[il.Upstream],
        fact_ads_stats: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per ad with its ad group, from the ad snapshots, completed from the facts."""
        return []

    @il.asset(
        partitioning=PARTITIONING,
        tags=["Dimension"],
        relations={"advertisers": il.Relation("asset", "tiktok_ads.advertisers", many=True, optional=True)},
    )
    def dim_accounts(
        self,
        context: il.ExecutionContext,
        advertisers: list[il.Upstream],
        fact_ads_stats: il.Upstream,
    ) -> list[dict[str, Any]]:
        """One row per advertiser, from the advertiser snapshots, completed from the facts."""
        return []
