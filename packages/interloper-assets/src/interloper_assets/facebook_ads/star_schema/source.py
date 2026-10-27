"""Facebook Ads star schema: views over every Facebook Ads account's tables."""

from __future__ import annotations

import ibis
import ibis.expr.types as ir
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest, union

from interloper_assets.facebook_ads.star_schema import schemas

CAMPAIGN_REPORT_COLUMNS = (
    "date_start",
    "campaign_id",
    "impressions",
    "clicks",
    "spend",
    "account_currency",
    "actions_purchase",
    "action_values_purchase",
)
AD_REPORT_COLUMNS = (
    "date_start",
    "campaign_id",
    "adset_id",
    "ad_id",
    "publisher_platform",
    "platform_position",
    "impression_device",
    "impressions",
    "clicks",
    "reach",
    "spend",
    "account_currency",
    "actions_purchase",
    "action_values_purchase",
)
INSTANT = "timestamp('UTC')"
ENTITY, REPORT, FACT = 1, 2, 3


def _account(upstream: il.Upstream) -> str:
    """The account an upstream's rows belong to; every view stamps it.

    Facebook's reports carry an account column, but the contract stamps the
    account from the source's discriminator on every platform, so the key is
    the same whether or not a vendor reports it.

    Args:
        upstream: A bound upstream of an account's source.

    Returns:
        The ad account id.

    Raises:
        ConfigError: If the upstream's asset belongs to no source, or its source has no discriminator.
    """
    source = upstream.asset.source
    account = source.discriminator if source is not None else None
    if account is None:
        raise ConfigError(f"Upstream '{upstream.asset.key}' belongs to no account")
    return account


def _local_date(column: ir.StringValue) -> ir.Value:
    """The date part of a local wall-clock timestamp (``2026-10-01T00:00:00+0200``); blank is NULL.

    Used by the campaign dimension for the scheduled start and end, which
    Facebook holds in the account's timezone: the date is read off the
    string, never converted through UTC.

    Args:
        column: A string column of ISO 8601 timestamps with an offset.

    Returns:
        A date column.
    """
    return column.nullif(ibis.literal("")).substr(0, 10).cast("date")


def _budget_type(table: ibis.Table) -> ir.Value:
    """Which budget a Facebook campaign sets: ``daily``, ``lifetime``, or NULL for neither.

    Args:
        table: A campaigns entity table.

    Returns:
        A string column.
    """
    lifetime = ibis.ifelse(table["lifetime_budget"].notnull(), "lifetime", ibis.null("string"))
    return ibis.ifelse(table["daily_budget"].notnull(), "daily", lifetime)


@il.source(
    tags=["Analytics"],
    icon="logos:facebook",
    maturity="alpha",
    group="facebook_ads",
    dataset="facebook_ads",
)
class FacebookAdsStarSchema(il.Source):
    """Star schema over every Facebook Ads account, as views beside the raw tables.

    The campaign fact unions each account's campaign report at campaign and
    day grain, with purchases under the account's attribution setting; the ad
    fact keeps the ad report's placement breakdown. Each dimension takes its
    rows from the entity snapshot where one is collected, else from the
    report, else from the fact's own keys, so every key a fact carries
    resolves. Every bound upstream's table must exist, or the view fails when
    it is created. Views are recreated on every run, so they follow the
    accounts bound to them; an account added to the organisation is absent
    until it is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={"campaigns_stats": il.Relation("asset", "facebook_ads.campaigns_stats", many=True, optional=True)},
    )
    def fact_campaign_performance(self, campaigns_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every account, one row per campaign and day.

        Key: date, account_id, campaign_id.
        """
        rows = union(campaigns_stats, *CAMPAIGN_REPORT_COLUMNS, account_id=_account)
        rows = rows.select(
            date=rows["date_start"],
            account_id=rows["account_id"],
            campaign_id=rows["campaign_id"],
            impressions=rows["impressions"],
            clicks=rows["clicks"],
            cost=rows["spend"],
            currency=rows["account_currency"],
            purchases=rows["actions_purchase"].cast("int64"),
            revenue=rows["action_values_purchase"].cast("float64"),
        )
        # the campaign report is broken down by placement, so reach (people, not additive across
        # rows) cannot be summed back to the campaign and stays on the ad fact only
        summed = rows.group_by(["date", "account_id", "campaign_id"]).aggregate(
            impressions=rows["impressions"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["cost"].sum(),
            currency=rows["currency"].max(),
            purchases=rows["purchases"].sum(),
            revenue=rows["revenue"].sum(),
        )
        return summed.select(
            "date",
            "account_id",
            "campaign_id",
            "impressions",
            "clicks",
            "cost",
            "currency",
            conversions=ibis.null("int64"),
            conversion_value=ibis.null("float64"),
            purchases=summed["purchases"],
            revenue=summed["revenue"],
        )

    @ibis_view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Ad performance of every account at the ad report's grain: per ad, day, platform, placement and device."""
        rows = union(ads_stats, *AD_REPORT_COLUMNS, account_id=_account)
        return rows.select(
            date=rows["date_start"],
            account_id=rows["account_id"],
            campaign_id=rows["campaign_id"],
            ad_group_id=rows["adset_id"],
            ad_id=rows["ad_id"],
            publisher_platform=rows["publisher_platform"],
            platform_position=rows["platform_position"],
            impression_device=rows["impression_device"],
            impressions=rows["impressions"],
            clicks=rows["clicks"],
            reach=rows["reach"],
            cost=rows["spend"],
            currency=rows["account_currency"],
            conversions=ibis.null("int64"),
            conversion_value=ibis.null("float64"),
            purchases=rows["actions_purchase"].cast("int64"),
            revenue=rows["action_values_purchase"].cast("float64"),
        )

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={
            "ad_accounts": il.Relation("asset", "facebook_ads.ad_accounts", many=True, optional=True),
            "campaigns_stats": il.Relation("asset", "facebook_ads.campaigns_stats", many=True, optional=True),
        },
    )
    def dim_accounts(
        self,
        ad_accounts: list[il.Upstream],
        campaigns_stats: list[il.Upstream],
        fact_campaign_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per ad account with its name, currency and timezone.

        Each account comes from its entity row, else the campaign report, else the facts.
        """

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                account_id=ibis.literal(_account(upstream)),
                account_name=table["name"],
                currency=table["currency"],
                timezone=table["timezone_name"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(table["date_start"]).aggregate(
                account_name=table["account_name"].max(), currency=table["account_currency"].max()
            )
            return days.select(
                account_id=ibis.literal(_account(upstream)),
                account_name=days["account_name"],
                currency=days["currency"],
                timezone=ibis.null("string"),
                as_of_date=days["date_start"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by([table["account_id"], table["date"]]).aggregate(currency=table["currency"].max())
            return days.select(
                account_id=days["account_id"],
                account_name=ibis.null("string"),
                currency=days["currency"],
                timezone=ibis.null("string"),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in ad_accounts),
            *(from_report(upstream) for upstream in campaigns_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "account_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation(
                "asset", ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"], many=True, optional=True
            )
        },
    )
    def dim_campaigns(self, campaigns: list[il.Upstream], fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per campaign, from its latest entity row, else its latest report, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                campaign_id=table["id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=table["name"],
                status=table["status"],
                objective=table["objective"],
                campaign_type=table["buying_type"],
                budget=ibis.null("float64"),
                budget_type=_budget_type(table),
                start_date=_local_date(table["start_time"]),
                end_date=_local_date(table["stop_time"]),
                created_at=table["created_time"],
                updated_at=table["updated_time"],
                as_of_date=table["date"],
                effective_status=table["effective_status"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by([table["date_start"], table["campaign_id"]]).aggregate(
                campaign_name=table["campaign_name"].max(), objective=table["objective"].max()
            )
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=days["campaign_name"],
                status=ibis.null("string"),
                objective=days["objective"],
                campaign_type=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date_start"],
                effective_status=ibis.null("string"),
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by([table["date"], table["campaign_id"]]).aggregate(account_id=table["account_id"].max())
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                campaign_name=ibis.null("string"),
                status=ibis.null("string"),
                objective=ibis.null("string"),
                campaign_type=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                effective_status=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        tiers = {"campaigns": from_entity, "campaigns_stats": from_report}
        rows = ibis.union(
            *(tiers[upstream.asset.key](upstream) for upstream in campaigns),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={
            "adsets": il.Relation("asset", "facebook_ads.adsets", many=True, optional=True),
            "ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_ad_groups(
        self, adsets: list[il.Upstream], ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream
    ) -> ibis.Table:
        """One row per ad set with its schedule and status: its entity row, else the ad report, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                ad_group_id=table["id"],
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=table["name"],
                status=table["status"],
                budget=ibis.null("float64"),
                budget_type=_budget_type(table),
                start_date=_local_date(table["start_time"]),
                end_date=_local_date(table["end_time"]),
                created_at=table["created_time"],
                updated_at=table["updated_time"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by([table["date_start"], table["adset_id"]]).aggregate(
                ad_group_name=table["adset_name"].max(), campaign_id=table["campaign_id"].max()
            )
            return days.select(
                ad_group_id=days["adset_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=days["ad_group_name"],
                status=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date_start"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by([table["date"], table["ad_group_id"]]).aggregate(
                campaign_id=table["campaign_id"].max(), account_id=table["account_id"].max()
            )
            return days.select(
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                ad_group_name=ibis.null("string"),
                status=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in adsets),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        return latest(rows, "ad_group_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={
            "ads": il.Relation("asset", "facebook_ads.ads", many=True, optional=True),
            "ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_ads(
        self, ads: list[il.Upstream], ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream
    ) -> ibis.Table:
        """One row per ad with its ad set and campaign: its latest entity row, else the ad report, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                ad_id=table["id"],
                ad_group_id=table["adset_id"],
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=table["name"],
                status=table["status"],
                created_at=table["created_time"],
                updated_at=table["updated_time"],
                as_of_date=table["date"],
                effective_status=table["effective_status"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by([table["date_start"], table["ad_id"]]).aggregate(
                ad_name=table["ad_name"].max(),
                ad_group_id=table["adset_id"].max(),
                campaign_id=table["campaign_id"].max(),
            )
            return days.select(
                ad_id=days["ad_id"],
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=days["ad_name"],
                status=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date_start"],
                effective_status=ibis.null("string"),
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by([table["date"], table["ad_id"]]).aggregate(
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
                status=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                effective_status=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in ads),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        return latest(rows, "ad_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")
