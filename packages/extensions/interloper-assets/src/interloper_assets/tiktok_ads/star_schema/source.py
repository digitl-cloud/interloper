"""TikTok Ads star schema: views over every TikTok Ads advertiser's tables."""

from __future__ import annotations

import ibis
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest, local_date, union

from interloper_assets.tiktok_ads.star_schema import schemas

REPORT_COLUMNS = (
    "stat_time_day",
    "campaign_id",
    "adgroup_id",
    "ad_id",
    "currency",
    "impressions",
    "clicks",
    "reach",
    "spend",
    "complete_payment",
    "total_complete_payment_rate",
)
INSTANT = "timestamp('UTC')"
ENTITY, REPORT, FACT = 1, 2, 3


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


@il.source(tags=["Analytics"], icon="logos:tiktok-icon", maturity="alpha", group="tiktok_ads", dataset="tiktok_ads")
class TiktokAdsStarSchema(il.Source):
    """Star schema over every TikTok Ads advertiser, as views beside the raw tables.

    Both facts read the ad report: the ad fact at its grain, the campaign fact
    summed to one row per campaign and day. Purchases are TikTok's complete
    payments under the advertiser's attribution setting. Each dimension takes
    its rows from the entity snapshot where one is collected, else from the
    ad report's names, else from the facts' keys, so every key a fact carries
    resolves. TikTok schedules the ad group, in UTC; its dates are read in the
    advertiser's own timezone. Every bound upstream's table must exist, or
    the view fails when it is created. Views are recreated on every run, so
    they follow the advertisers bound to them; an advertiser added to the
    organisation is absent until it is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "tiktok_ads.ads_stats", many=True, optional=True)},
    )
    def fact_campaign_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every advertiser, one row per campaign and day.

        Key: date, account_id, campaign_id.
        """
        rows = union(ads_stats, *REPORT_COLUMNS, account_id=_account)
        rows = rows.filter(rows["campaign_id"].notnull())
        summed = rows.group_by(["stat_time_day", "account_id", "campaign_id"]).aggregate(
            impressions=rows["impressions"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["spend"].sum(),
            currency=rows["currency"].max(),
            purchases=rows["complete_payment"].sum(),
            revenue=rows["total_complete_payment_rate"].sum(),
        )
        return summed.select(
            date=summed["stat_time_day"],
            account_id=summed["account_id"],
            campaign_id=summed["campaign_id"],
            impressions=summed["impressions"],
            clicks=summed["clicks"],
            cost=summed["cost"],
            currency=summed["currency"],
            purchases=summed["purchases"],
            revenue=summed["revenue"],
        )

    @ibis_view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "tiktok_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> ibis.Table:
        """Ad performance of every advertiser, one row per ad and day.

        Key: date, account_id, ad_id.
        """
        rows = union(ads_stats, *REPORT_COLUMNS, account_id=_account)
        rows = rows.filter(rows["ad_id"].notnull())
        summed = rows.group_by(["stat_time_day", "account_id", "ad_id"]).aggregate(
            campaign_id=rows["campaign_id"].max(),
            ad_group_id=rows["adgroup_id"].max(),
            impressions=rows["impressions"].sum(),
            clicks=rows["clicks"].sum(),
            reach=rows["reach"].sum(),
            cost=rows["spend"].sum(),
            currency=rows["currency"].max(),
            purchases=rows["complete_payment"].sum(),
            revenue=rows["total_complete_payment_rate"].sum(),
        )
        return summed.select(
            date=summed["stat_time_day"],
            account_id=summed["account_id"],
            campaign_id=summed["campaign_id"],
            ad_group_id=summed["ad_group_id"],
            ad_id=summed["ad_id"],
            impressions=summed["impressions"],
            clicks=summed["clicks"],
            reach=summed["reach"],
            cost=summed["cost"],
            currency=summed["currency"],
            purchases=summed["purchases"],
            revenue=summed["revenue"],
        )

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={"advertisers": il.Relation("asset", "tiktok_ads.advertisers", many=True, optional=True)},
    )
    def dim_accounts(self, advertisers: list[il.Upstream], fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per advertiser with its name, currency and timezone: its latest snapshot, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                account_id=ibis.literal(_account(upstream)),
                account_name=table["name"],
                currency=table["currency"],
                timezone=table["timezone"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["account_id", "date"]).aggregate(currency=table["currency"].max())
            return days.select(
                account_id=days["account_id"],
                account_name=ibis.null("string"),
                currency=days["currency"],
                timezone=ibis.null("string"),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in advertisers),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "account_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation("asset", "tiktok_ads.campaigns", many=True, optional=True),
            "ads_stats": il.Relation("asset", "tiktok_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_campaigns(
        self, campaigns: list[il.Upstream], ads_stats: list[il.Upstream], fact_campaign_performance: il.Upstream
    ) -> ibis.Table:
        """One row per campaign: its latest snapshot, else its latest report name, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=table["campaign_name"],
                status=table["operation_status"],
                objective=table["objective_type"],
                campaign_type=table["campaign_type"],
                budget=table["budget"],
                budget_type=table["budget_mode"],
                created_at=table["create_time"],
                updated_at=table["modify_time"],
                as_of_date=table["date"],
                effective_status=table["secondary_status"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["stat_time_day", "campaign_id"]).aggregate(
                campaign_name=table["campaign_name"].max()
            )
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=days["campaign_name"],
                status=ibis.null("string"),
                objective=ibis.null("string"),
                campaign_type=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["stat_time_day"],
                effective_status=ibis.null("string"),
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["date", "campaign_id"]).aggregate(account_id=table["account_id"].max())
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                campaign_name=ibis.null("string"),
                status=ibis.null("string"),
                objective=ibis.null("string"),
                campaign_type=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                effective_status=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in campaigns),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        kept = latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc())
        return kept.select(
            "campaign_id",
            "account_id",
            "campaign_name",
            "status",
            "objective",
            "campaign_type",
            "budget",
            "budget_type",
            "created_at",
            "updated_at",
            "as_of_date",
            "effective_status",
        )

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={
            "adgroups": il.Relation("asset", "tiktok_ads.adgroups", many=True, optional=True),
            "ads_stats": il.Relation("asset", "tiktok_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_ad_groups(
        self,
        adgroups: list[il.Upstream],
        ads_stats: list[il.Upstream],
        fact_ad_performance: il.Upstream,
        dim_accounts: il.Upstream,
    ) -> ibis.Table:
        """One row per ad group with its schedule: its latest snapshot, else its latest report name, else the facts."""
        timezones = dim_accounts.data.select(zone_account_id="account_id", timezone="timezone")

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data.select(
                "date",
                "adgroup_id",
                "campaign_id",
                "adgroup_name",
                "operation_status",
                "secondary_status",
                "budget",
                "budget_mode",
                "schedule_start_time",
                "schedule_end_time",
                "create_time",
                "modify_time",
                account_id=ibis.literal(_account(upstream)),
            )
            table = table.left_join(timezones, table["account_id"] == timezones["zone_account_id"])
            return table.select(
                ad_group_id=table["adgroup_id"],
                campaign_id=table["campaign_id"],
                account_id=table["account_id"],
                ad_group_name=table["adgroup_name"],
                status=table["operation_status"],
                budget=table["budget"],
                budget_type=table["budget_mode"],
                start_date=local_date(table["schedule_start_time"], table["timezone"]),
                end_date=local_date(table["schedule_end_time"], table["timezone"]),
                created_at=table["create_time"],
                updated_at=table["modify_time"],
                as_of_date=table["date"],
                effective_status=table["secondary_status"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["stat_time_day", "adgroup_id"]).aggregate(
                campaign_id=table["campaign_id"].max(), ad_group_name=table["adgroup_name"].max()
            )
            return days.select(
                ad_group_id=days["adgroup_id"],
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
                as_of_date=days["stat_time_day"],
                effective_status=ibis.null("string"),
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

        rows = ibis.union(
            *(from_entity(upstream) for upstream in adgroups),
            *(from_report(upstream) for upstream in ads_stats),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        return latest(rows, "ad_group_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={
            "ads": il.Relation("asset", "tiktok_ads.ads", many=True, optional=True),
            "ads_stats": il.Relation("asset", "tiktok_ads.ads_stats", many=True, optional=True),
        },
    )
    def dim_ads(
        self, ads: list[il.Upstream], ads_stats: list[il.Upstream], fact_ad_performance: il.Upstream
    ) -> ibis.Table:
        """One row per ad: its latest snapshot, else its latest report name, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                ad_id=table["ad_id"],
                ad_group_id=table["adgroup_id"],
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=table["ad_name"],
                status=table["operation_status"],
                created_at=table["create_time"],
                updated_at=table["modify_time"],
                as_of_date=table["date"],
                effective_status=table["secondary_status"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["stat_time_day", "ad_id"]).aggregate(
                ad_group_id=table["adgroup_id"].max(),
                campaign_id=table["campaign_id"].max(),
                ad_name=table["ad_name"].max(),
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
                as_of_date=days["stat_time_day"],
                effective_status=ibis.null("string"),
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
