"""Bing Ads star schema: views over every Bing Ads account's ad report."""

from __future__ import annotations

import ibis
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest, union

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.star_schema import schemas

MEASURE_COLUMNS = ("impressions", "clicks", "spend", "conversions", "revenue", "currency_code")
INSTANT = "timestamp('UTC')"
ENTITY, REPORT, FACT = 1, 2, 3


def _account(upstream: il.Upstream) -> str:
    """The account an upstream's rows belong to; every view stamps it.

    Args:
        upstream: A bound upstream of an account's source.

    Returns:
        The account id.

    Raises:
        ConfigError: If the upstream's asset belongs to no source, or its source has no discriminator.
    """
    source = upstream.asset.source
    account = source.discriminator if source is not None else None
    if account is None:
        raise ConfigError(f"Upstream '{upstream.asset.key}' belongs to no account")
    return account


@il.source(tags=["Analytics"], icon="icon:bing", maturity="alpha", group="bing_ads", dataset="bing_ads")
class BingAdsStarSchema(il.Source):
    """Star schema over every Bing Ads account, as views beside the raw tables.

    Both facts read the ad report, which Bing splits by device, network,
    match type and conversion goal; the ad fact sums it to one row per ad and
    day, the campaign fact to one row per campaign and day. A goal's
    conversions arrive on rows of their own with no delivery, so the sums
    count nothing twice. Conversions are the goals the advertiser counts in
    Bing's Conversions; Bing does not say which goals are purchases, so the
    facts publish no purchases. Days are each campaign's own, in the time
    zone Bing reports it in: its account's, unless the campaign was created
    in another.
    The dimensions take each account, campaign, ad group and ad from its
    latest entity snapshot where one is collected, else from its latest
    report, else from the facts' keys. Only the snapshots know an account's
    time zone, a campaign's budget, an ad group's schedule and when anything
    last changed; Bing schedules ad groups, not campaigns, so campaigns
    publish no schedule, and Bing gives no creation time, so nothing
    publishes one. An ad's campaign comes from its ad group, since the ad's
    own snapshot names only the ad group.
    Every bound upstream's table must exist, or the view fails when it is
    created. Views are recreated on every run, so they follow the accounts
    bound to them; an account added to the organisation is absent until it
    is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True)},
    )
    def fact_campaign_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every account, one row per campaign and day.

        Key: date, account_id, campaign_id.
        """
        rows = union(ads_stats, "time_period", "campaign_id", *MEASURE_COLUMNS, account_id=_account)
        rows = rows.filter(rows["campaign_id"].notnull())
        summed = rows.group_by(["time_period", "account_id", "campaign_id"]).aggregate(
            impressions=rows["impressions"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["spend"].sum(),
            currency=rows["currency_code"].max(),
            conversions=rows["conversions"].sum(),
            conversion_value=rows["revenue"].sum(),
        )
        return summed.select(
            date=summed["time_period"],
            account_id=summed["account_id"],
            campaign_id=summed["campaign_id"],
            impressions=summed["impressions"],
            clicks=summed["clicks"],
            cost=summed["cost"],
            currency=summed["currency"],
            conversions=summed["conversions"],
            conversion_value=summed["conversion_value"],
        )

    @ibis_view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Ad performance of every account, one row per ad and day.

        Key: date, account_id, ad_id.
        """
        rows = union(
            ads_stats, "time_period", "campaign_id", "ad_group_id", "ad_id", *MEASURE_COLUMNS, account_id=_account
        )
        rows = rows.filter(rows["ad_id"].notnull())
        summed = rows.group_by(["time_period", "account_id", "ad_id"]).aggregate(
            campaign_id=rows["campaign_id"].max(),
            ad_group_id=rows["ad_group_id"].max(),
            impressions=rows["impressions"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["spend"].sum(),
            currency=rows["currency_code"].max(),
            conversions=rows["conversions"].sum(),
            conversion_value=rows["revenue"].sum(),
        )
        return summed.select(
            date=summed["time_period"],
            account_id=summed["account_id"],
            campaign_id=summed["campaign_id"],
            ad_group_id=summed["ad_group_id"],
            ad_id=summed["ad_id"],
            impressions=summed["impressions"],
            clicks=summed["clicks"],
            cost=summed["cost"],
            currency=summed["currency"],
            conversions=summed["conversions"],
            conversion_value=summed["conversion_value"],
        )

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={
            "accounts": il.Relation("asset", "bing_ads.accounts", many=True, optional=True),
            "ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_accounts(
        self,
        accounts: list[il.Upstream],
        ads_stats: list[il.Upstream],
        fact_campaign_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per account with its name, currency, time zone and customer.

        Each account comes from its entity snapshot, else its latest report, else the facts.
        """

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                account_id=ibis.literal(_account(upstream)),
                account_name=table["name"],
                currency=table["currency_code"],
                timezone=table["time_zone"].substitute(constants.TIME_ZONES, else_=ibis.null("string")),
                as_of_date=table["date"],
                customer_id=table["parent_customer_id"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by("time_period").aggregate(
                account_name=table["account_name"].max(),
                currency=table["currency_code"].max(),
                customer_id=table["customer_id"].max(),
            )
            return days.select(
                account_id=ibis.literal(_account(upstream)),
                account_name=days["account_name"],
                currency=days["currency"],
                timezone=ibis.null("string"),
                as_of_date=days["time_period"],
                customer_id=days["customer_id"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["account_id", "date"]).aggregate(currency=table["currency"].max())
            return days.select(
                account_id=days["account_id"],
                account_name=ibis.null("string"),
                currency=days["currency"],
                timezone=ibis.null("string"),
                as_of_date=days["date"],
                customer_id=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in accounts),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "account_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation("asset", "bing_ads.campaigns", many=True, optional=True),
            "ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_campaigns(
        self,
        campaigns: list[il.Upstream],
        ads_stats: list[il.Upstream],
        fact_campaign_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per campaign with its name, status, type and budget.

        Each campaign comes from its entity snapshot, else its latest report, else the facts.
        """

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                campaign_id=table["id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=table["campaign"],
                status=table["status"],
                campaign_type=table["campaign_type"],
                budget=table["budget"],
                budget_type=table["budget_type"],
                updated_at=table["modified_time"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["time_period", "campaign_id"]).aggregate(
                campaign_name=table["campaign_name"].max(),
                status=table["campaign_status"].max(),
                campaign_type=table["campaign_type"].max(),
            )
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=days["campaign_name"],
                status=days["status"],
                campaign_type=days["campaign_type"],
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["time_period"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["date", "campaign_id"]).aggregate(account_id=table["account_id"].max())
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                campaign_name=ibis.null("string"),
                status=ibis.null("string"),
                campaign_type=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in campaigns),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={
            "ad_groups": il.Relation("asset", "bing_ads.ad_groups", many=True, optional=True),
            "ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_ad_groups(
        self,
        ad_groups: list[il.Upstream],
        ads_stats: list[il.Upstream],
        fact_ad_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per ad group with its name, campaign, status and schedule.

        Each ad group comes from its entity snapshot, else its latest report, else the facts.
        """

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                ad_group_id=table["id"],
                campaign_id=table["parent_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=table["ad_group"],
                status=table["status"],
                start_date=table["start_date"],
                end_date=table["end_date"],
                updated_at=table["modified_time"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["time_period", "ad_group_id"]).aggregate(
                campaign_id=table["campaign_id"].max(),
                ad_group_name=table["ad_group_name"].max(),
                status=table["ad_group_status"].max(),
            )
            return days.select(
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=days["ad_group_name"],
                status=days["status"],
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["time_period"],
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
                status=ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in ad_groups),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        return latest(rows, "ad_group_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={
            "ads": il.Relation("asset", "bing_ads.ads", many=True, optional=True),
            "ads_stats": il.Relation("asset", "bing_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_ads(
        self,
        ads: list[il.Upstream],
        ads_stats: list[il.Upstream],
        fact_ad_performance: il.Upstream,
        dim_ad_groups: il.Upstream,
    ) -> ibis.Table:
        """One row per ad with its ad group, campaign, status, type and headline.

        Each ad comes from its entity snapshot, else its latest report, else the facts.
        """
        groups = dim_ad_groups.data.select("ad_group_id", "campaign_id")

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            # the ad's snapshot names its ad group only; the campaign is the ad group's
            joined = table.left_join(groups, table["parent_id"] == groups["ad_group_id"])
            return joined.select(
                ad_id=table["id"],
                ad_group_id=table["parent_id"],
                campaign_id=groups["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                status=table["status"],
                updated_at=table["modified_time"],
                as_of_date=table["date"],
                ad_type=table["type"],
                # only a responsive ad's Headline is one headline; a responsive search ad's lists its assets
                headline=(table["type"] == "Responsive Ad").ifelse(table["headline"], ibis.null("string")),
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["time_period", "ad_id"]).aggregate(
                ad_group_id=table["ad_group_id"].max(),
                campaign_id=table["campaign_id"].max(),
                status=table["ad_status"].max(),
                ad_type=table["ad_type"].max(),
                headline=table["headline"].max(),
            )
            return days.select(
                ad_id=days["ad_id"],
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                status=days["status"],
                updated_at=ibis.null(INSTANT),
                as_of_date=days["time_period"],
                ad_type=days["ad_type"],
                headline=days["headline"],
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
                status=ibis.null("string"),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                ad_type=ibis.null("string"),
                headline=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in ads),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        return latest(rows, "ad_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")
