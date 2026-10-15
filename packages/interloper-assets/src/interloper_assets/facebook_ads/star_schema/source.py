"""Facebook Ads star schema: views over every Facebook Ads account's tables."""

from __future__ import annotations

import ibis
import interloper as il
from interloper_sql import ibis_view, union

from interloper_assets.facebook_ads.star_schema import schemas

FACT_COLUMNS = (
    "date_start",
    "account_id",
    "account_name",
    "account_currency",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "publisher_platform",
    "platform_position",
    "impression_device",
    "impressions",
    "clicks",
    "reach",
    "spend",
)


def _latest(rows: ibis.Table, key: str, *order: ibis.Expr) -> ibis.Table:
    """One row per *key*, the first in *order*; shared by the three dimensions.

    Args:
        rows: Candidate rows, several per key.
        key: The column one row is kept per value of.
        *order: The sort that puts the row to keep first, such as the source
            tier then the latest date.

    Returns:
        The kept rows, without the ranking column.
    """
    ranked = rows.mutate(rank=ibis.row_number().over(group_by=key, order_by=list(order)))
    return ranked.filter(ranked["rank"] == 0).drop("rank")


@il.source(tags=["Analytics"], icon="logos:facebook", maturity="alpha", group="facebook_ads")
class FacebookAdsStarSchema(il.Source):
    """Star schema over every Facebook Ads account, as views in the warehouse.

    The fact unions each account's ad performance; the dimensions take each
    campaign from its latest entity row, else its latest report, else the
    facts, and each ad from its latest entity row, else the facts, so every
    key a fact carries resolves. Every bound upstream's
    table must exist, or the view fails when it is created. Views are
    recreated on every run, so they follow the accounts bound to them.
    """

    @ibis_view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Ad performance of every account, one row per ad, day and placement breakdown."""
        return union(ads_stats, *FACT_COLUMNS).rename(date="date_start")

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation(
                "asset", ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"], many=True, optional=True
            )
        },
    )
    def dim_campaigns(self, campaigns: list[il.Upstream], fact_ad_performance: il.Upstream) -> ibis.Table:
        """One row per campaign, from its latest entity row, else its latest report, else the facts."""

        def from_entity(table: ibis.Table) -> ibis.Table:
            return table.select(
                campaign_id=table["id"],
                campaign_name=table["name"],
                account_id=table["account_id"],
                objective=table["objective"],
                status=table["status"],
                date=table["date"],
                priority=ibis.literal(1),
            )

        def from_report(table: ibis.Table) -> ibis.Table:
            return table.select(
                campaign_id=table["campaign_id"].cast("string"),
                campaign_name=table["campaign_name"],
                account_id=table["account_id"].cast("string"),
                objective=table["objective"],
                status=ibis.null("string"),
                date=table["date_start"],
                priority=ibis.literal(2),
            )

        def from_facts(table: ibis.Table) -> ibis.Table:
            return table.select(
                campaign_id=table["campaign_id"],
                campaign_name=table["campaign_name"],
                account_id=table["account_id"],
                objective=ibis.null("string"),
                status=ibis.null("string"),
                date=table["date"],
                priority=ibis.literal(3),
            )

        candidates = {"campaigns": from_entity, "campaigns_stats": from_report}
        rows = ibis.union(
            *(candidates[upstream.asset.key](upstream.data) for upstream in campaigns),
            from_facts(fact_ad_performance.data),
            distinct=False,
        )
        return _latest(rows, "campaign_id", rows["priority"].asc(), rows["date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={"ads": il.Relation("asset", "facebook_ads.ads", many=True, optional=True)},
    )
    def dim_ads(self, ads: list[il.Upstream], fact_ad_performance: il.Upstream) -> ibis.Table:
        """One row per ad with its ad set, from its latest entity row, else the facts."""

        def from_entity(table: ibis.Table) -> ibis.Table:
            return table.select(
                ad_id=table["id"],
                ad_name=table["name"],
                adset_id=table["adset_id"],
                adset_name=ibis.null("string"),
                campaign_id=table["campaign_id"],
                account_id=table["account_id"],
                status=table["status"],
                date=table["date"],
                priority=ibis.literal(1),
            )

        def from_facts(table: ibis.Table) -> ibis.Table:
            return table.select(
                ad_id=table["ad_id"],
                ad_name=table["ad_name"],
                adset_id=table["adset_id"],
                adset_name=table["adset_name"],
                campaign_id=table["campaign_id"],
                account_id=table["account_id"],
                status=ibis.null("string"),
                date=table["date"],
                priority=ibis.literal(2),
            )

        rows = ibis.union(
            *(from_entity(upstream.data) for upstream in ads), from_facts(fact_ad_performance.data), distinct=False
        )
        return _latest(rows, "ad_id", rows["priority"].asc(), rows["date"].desc()).drop("priority")

    @ibis_view(schema=schemas.DimAccounts, tags=["Dimension"])
    def dim_accounts(self, fact_ad_performance: il.Upstream) -> ibis.Table:
        """One row per ad account with its name and currency, from the facts."""
        rows = fact_ad_performance.data.select("account_id", "account_name", "account_currency", "date")
        return _latest(rows, "account_id", rows["date"].desc())
