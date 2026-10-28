"""Criteo star schema: views over every Criteo advertiser's tables."""

from __future__ import annotations

import ibis
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest, union

from interloper_assets.criteo.star_schema import schemas

MEASURE_COLUMNS = (
    "displays",
    "clicks",
    "advertiser_cost",
    "currency",
    "sales_client_attribution",
    "revenue_generated_client_attribution",
)
REPORT, FACT = 1, 2


def _account(upstream: il.Upstream) -> str:
    """The advertiser an upstream's rows belong to; every view stamps it.

    Args:
        upstream: A bound upstream of an advertiser's source.

    Returns:
        The advertiser id.

    Raises:
        ConfigError: If the upstream's asset belongs to no source, or its source has no discriminator.
    """
    source = upstream.asset.source
    account = source.discriminator if source is not None else None
    if account is None:
        raise ConfigError(f"Upstream '{upstream.asset.key}' belongs to no account")
    return account


@il.source(tags=["Analytics"], icon="icon:criteo", maturity="alpha", group="criteo", dataset="criteo")
class CriteoStarSchema(il.Source):
    """Star schema over every Criteo advertiser, as views beside the raw tables.

    The campaign fact unions each advertiser's campaign report, one row per
    campaign and day; the ad fact does the same at the ad report's grain.
    Days are Criteo's report days, in UTC. Purchases are Criteo's sales under
    the advertiser's own attribution setting, its primary conversion. The
    reports name every campaign, ad set and ad, so the dimensions take their
    names from the reports and fall back to the facts' keys, so every key a
    fact carries resolves. Statuses, budgets and schedules come only from
    Criteo's entity endpoints, which are not collected yet, so the dimensions
    publish no such column. Every bound upstream's table must exist, or the view
    fails when it is created. Views are recreated on every run, so they
    follow the advertisers bound to them; an advertiser added to the
    organisation is absent until it is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={"campaigns_stats": il.Relation("asset", "criteo.campaigns_stats", many=True, optional=True)},
    )
    def fact_campaign_performance(self, campaigns_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every advertiser, one row per campaign and day.

        Key: date, account_id, campaign_id.
        """
        rows = union(campaigns_stats, "day", "campaign_id", *MEASURE_COLUMNS, account_id=_account)
        summed = rows.group_by(["day", "account_id", "campaign_id"]).aggregate(
            impressions=rows["displays"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["advertiser_cost"].sum(),
            currency=rows["currency"].max(),
            sales=rows["sales_client_attribution"].sum(),
            revenue=rows["revenue_generated_client_attribution"].sum(),
        )
        return summed.select(
            date=summed["day"],
            account_id=summed["account_id"],
            campaign_id=summed["campaign_id"],
            impressions=summed["impressions"],
            clicks=summed["clicks"],
            cost=summed["cost"],
            currency=summed["currency"],
            conversions=summed["sales"],
            conversion_value=summed["revenue"],
            purchases=summed["sales"],
            revenue=summed["revenue"],
        )

    @ibis_view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "criteo.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Ad performance of every advertiser, one row per ad and day.

        Key: date, account_id, ad_id.
        """
        rows = union(ads_stats, "day", "campaign_id", "adset_id", "ad_id", *MEASURE_COLUMNS, account_id=_account)
        summed = rows.group_by(["day", "account_id", "ad_id"]).aggregate(
            campaign_id=rows["campaign_id"].max(),
            ad_group_id=rows["adset_id"].max(),
            impressions=rows["displays"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["advertiser_cost"].sum(),
            currency=rows["currency"].max(),
            sales=rows["sales_client_attribution"].sum(),
            revenue=rows["revenue_generated_client_attribution"].sum(),
        )
        return summed.select(
            date=summed["day"],
            account_id=summed["account_id"],
            campaign_id=summed["campaign_id"],
            ad_group_id=summed["ad_group_id"],
            ad_id=summed["ad_id"],
            impressions=summed["impressions"],
            clicks=summed["clicks"],
            cost=summed["cost"],
            currency=summed["currency"],
            conversions=summed["sales"],
            conversion_value=summed["revenue"],
            purchases=summed["sales"],
            revenue=summed["revenue"],
        )

    @ibis_view(schema=schemas.DimAccounts, tags=["Dimension"])
    def dim_accounts(self, fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per advertiser with the currency its reports state, from the facts."""
        table = fact_campaign_performance.data
        days = table.group_by(["account_id", "date"]).aggregate(currency=table["currency"].max())
        rows = days.select(
            account_id=days["account_id"],
            currency=days["currency"],
            as_of_date=days["date"],
        )
        return latest(rows, "account_id", rows["as_of_date"].desc())

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={"campaigns_stats": il.Relation("asset", "criteo.campaigns_stats", many=True, optional=True)},
    )
    def dim_campaigns(self, campaigns_stats: list[il.Upstream], fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per campaign with its name, from its latest report, else the facts."""

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["day", "campaign_id"]).aggregate(campaign_name=table["campaign"].max())
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=days["campaign_name"],
                as_of_date=days["day"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["date", "campaign_id"]).aggregate(account_id=table["account_id"].max())
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                campaign_name=ibis.null("string"),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_report(upstream) for upstream in campaigns_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        kept = latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc())
        return kept.select(
            "campaign_id",
            "account_id",
            "campaign_name",
            "as_of_date",
        )

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={"ads_stats": il.Relation("asset", "criteo.ads_stats", many=True, optional=True)},
    )
    def dim_ad_groups(self, ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream) -> ibis.Table:
        """One row per ad set with its name and campaign, from its latest ad report, else the facts."""

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["day", "adset_id"]).aggregate(
                campaign_id=table["campaign_id"].max(), ad_group_name=table["adset"].max()
            )
            return days.select(
                ad_group_id=days["adset_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=days["ad_group_name"],
                as_of_date=days["day"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["date", "ad_group_id"]).aggregate(
                campaign_id=table["campaign_id"].max(), account_id=table["account_id"].max()
            )
            return days.select(
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                ad_group_name=ibis.null("string"),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        kept = latest(rows, "ad_group_id", rows["priority"].asc(), rows["as_of_date"].desc())
        return kept.select(
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_group_name",
            "as_of_date",
        )

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={"ads_stats": il.Relation("asset", "criteo.ads_stats", many=True, optional=True)},
    )
    def dim_ads(self, ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream) -> ibis.Table:
        """One row per ad with its name, ad set and campaign, from its latest ad report, else the facts."""

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["day", "ad_id"]).aggregate(
                ad_group_id=table["adset_id"].max(),
                campaign_id=table["campaign_id"].max(),
                ad_name=table["ad"].max(),
            )
            return days.select(
                ad_id=days["ad_id"],
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=days["ad_name"],
                as_of_date=days["day"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["date", "ad_id"]).aggregate(
                ad_group_id=table["ad_group_id"].max(),
                campaign_id=table["campaign_id"].max(),
                account_id=table["account_id"].max(),
            )
            return days.select(
                ad_id=days["ad_id"],
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                ad_name=ibis.null("string"),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        kept = latest(rows, "ad_id", rows["priority"].asc(), rows["as_of_date"].desc())
        return kept.select(
            "ad_id",
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_name",
            "as_of_date",
        )
