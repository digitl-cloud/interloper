"""Facebook Ads star schema: views over every Facebook Ads account's tables."""

from __future__ import annotations

import interloper as il
from interloper.errors import ConfigError

from interloper_assets.facebook_ads_star_schema import schemas


def _union(upstreams: list[il.Upstream], select_by_key: dict[str, str]) -> str:
    """One SELECT per upstream, by its asset key, joined by UNION ALL.

    Args:
        upstreams: The bound upstreams, each carrying a table reference.
        select_by_key: The column list to select from an upstream, keyed by its asset key.

    Returns:
        The UNION ALL of one SELECT per upstream.

    Raises:
        ConfigError: If no upstream is bound; a view over nothing has no columns.
    """
    if not upstreams:
        raise ConfigError("no upstream bound: a star schema view needs at least one account")
    return " UNION ALL ".join(
        f"SELECT {select_by_key[upstream.asset.key]} FROM {upstream.data.name}" for upstream in upstreams
    )


FACT_COLUMNS = (
    "date_start AS date, CAST(account_id AS STRING) AS account_id, account_name, account_currency, "
    "CAST(campaign_id AS STRING) AS campaign_id, campaign_name, CAST(adset_id AS STRING) AS adset_id, adset_name, "
    "CAST(ad_id AS STRING) AS ad_id, ad_name, publisher_platform, platform_position, impression_device, "
    "impressions, clicks, reach, spend"
)

CAMPAIGN_CANDIDATES = {
    "campaigns": (
        "CAST(id AS STRING) AS campaign_id, name AS campaign_name, CAST(account_id AS STRING) AS account_id, "
        "objective, status, date, 1 AS priority"
    ),
    "campaigns_stats": (
        "CAST(campaign_id AS STRING) AS campaign_id, campaign_name, CAST(account_id AS STRING) AS account_id, "
        "objective, CAST(NULL AS STRING) AS status, date_start AS date, 2 AS priority"
    ),
}

AD_CANDIDATES = {
    "ads": (
        "CAST(id AS STRING) AS ad_id, name AS ad_name, CAST(adset_id AS STRING) AS adset_id, "
        "CAST(NULL AS STRING) AS adset_name, CAST(campaign_id AS STRING) AS campaign_id, "
        "CAST(account_id AS STRING) AS account_id, status, date, 1 AS priority"
    ),
}


@il.source(tags=["Analytics"], icon="logos:facebook", maturity="alpha")
class FacebookAdsStarSchema(il.Source):
    """Star schema over every Facebook Ads account, as views in the warehouse.

    The fact unions each account's ad performance; the dimensions take each
    campaign from its latest snapshot, else its latest report, else the
    facts, and each ad from its latest snapshot, else the facts, so every
    key a fact carries resolves. Every bound upstream's
    table must exist, or the view fails when it is created. Views are
    recreated on every run, so they follow the accounts bound to them.
    """

    @il.view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> str:
        """Ad performance of every account, one row per ad, day and placement breakdown."""
        return _union(ads_stats, {"ads_stats": FACT_COLUMNS})

    @il.view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation(
                "asset", ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"], many=True, optional=True
            )
        },
    )
    def dim_campaigns(self, campaigns: list[il.Upstream], fact_ad_performance: il.Upstream) -> str:
        """One row per campaign, from its latest snapshot, else its latest report, else the facts."""
        candidates = [
            f"SELECT {CAMPAIGN_CANDIDATES[upstream.asset.key]} FROM {upstream.data.name}" for upstream in campaigns
        ]
        candidates.append(
            "SELECT campaign_id, campaign_name, account_id, CAST(NULL AS STRING) AS objective, "
            f"CAST(NULL AS STRING) AS status, date, 3 AS priority FROM {fact_ad_performance.data.name}"
        )
        return (
            "SELECT campaign_id, campaign_name, account_id, objective, status, date "
            f"FROM ({' UNION ALL '.join(candidates)}) WHERE TRUE "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY priority, date DESC) = 1"
        )

    @il.view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={"ads": il.Relation("asset", "facebook_ads.ads", many=True, optional=True)},
    )
    def dim_ads(self, ads: list[il.Upstream], fact_ad_performance: il.Upstream) -> str:
        """One row per ad with its ad set, from its latest snapshot, else the facts."""
        candidates = [f"SELECT {AD_CANDIDATES[upstream.asset.key]} FROM {upstream.data.name}" for upstream in ads]
        candidates.append(
            "SELECT ad_id, ad_name, adset_id, adset_name, campaign_id, account_id, CAST(NULL AS STRING) AS status, "
            f"date, 2 AS priority FROM {fact_ad_performance.data.name}"
        )
        return (
            "SELECT ad_id, ad_name, adset_id, adset_name, campaign_id, account_id, status, date "
            f"FROM ({' UNION ALL '.join(candidates)}) WHERE TRUE "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY priority, date DESC) = 1"
        )

    @il.view(schema=schemas.DimAccounts, tags=["Dimension"])
    def dim_accounts(self, fact_ad_performance: il.Upstream) -> str:
        """One row per ad account with its name and currency, from the facts."""
        return (
            "SELECT account_id, account_name, account_currency, date "
            f"FROM {fact_ad_performance.data.name} WHERE TRUE "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY date DESC) = 1"
        )
