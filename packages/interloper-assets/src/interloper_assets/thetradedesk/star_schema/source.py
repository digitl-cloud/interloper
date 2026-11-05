"""The Trade Desk star schema: views over every The Trade Desk partner's tables."""

from __future__ import annotations

import ibis
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest, local_date, union

from interloper_assets.thetradedesk.constants import REPORT_TIMEZONE
from interloper_assets.thetradedesk.star_schema import schemas

REPORT_COLUMNS = (
    "date",
    "campaign_id",
    "ad_group_id",
    "impressions",
    "clicks",
    "advertiser_cost_adv_currency",
    "advertiser_currency_code",
)
INSTANT = "timestamp('UTC')"
ENTITY, REPORT, FACT = 1, 2, 3


def _account(upstream: il.Upstream) -> str:
    """The partner an upstream's rows belong to; every view stamps it.

    Args:
        upstream: A bound upstream of a partner's source.

    Returns:
        The partner id.

    Raises:
        ConfigError: If the upstream's asset belongs to no source, or its source has no discriminator.
    """
    source = upstream.asset.source
    account = source.discriminator if source is not None else None
    if account is None:
        raise ConfigError(f"Upstream '{upstream.asset.key}' belongs to no account")
    return account


def _performance(ad_groups_stats: list[il.Upstream], grain: str) -> ibis.Table:
    """The report's rows with a *grain* id, the keys and measures every fact sums.

    Used by both facts: the report's rows are finer than either fact (one per
    creative, deal and frequency bucket), and rows without the grain's id
    identify nothing, so they are dropped before anything is summed.

    Args:
        ad_groups_stats: The bound ad group reports, one per partner.
        grain: The id column the fact is keyed on, ``campaign_id`` or ``ad_group_id``.

    Returns:
        The report's columns with the partner stamped as ``account_id``.
    """
    rows = union(ad_groups_stats, *REPORT_COLUMNS, account_id=_account)
    return rows.filter(rows[grain].notnull())


def _measures(rows: ibis.Table) -> dict[str, ibis.Expr]:
    """The facts' measures, summed over the report's rows of one key and day.

    Used by both facts, so they sum the same columns the same way: cost is
    the advertiser cost in the advertiser's currency, which one campaign's
    rows share since a campaign belongs to one advertiser.

    Args:
        rows: The report's rows, grouped by the fact's key.

    Returns:
        The aggregations, by output column.
    """
    return {
        "impressions": rows["impressions"].sum(),
        "clicks": rows["clicks"].sum(),
        "cost": rows["advertiser_cost_adv_currency"].sum(),
        "currency": rows["advertiser_currency_code"].max(),
    }


@il.source(
    tags=["Analytics"], icon="icon:thetradedesk", maturity="alpha", group="the_trade_desk", dataset="the_trade_desk"
)
class TheTradeDeskStarSchema(il.Source):
    """Star schema over every The Trade Desk partner, as views beside the raw tables.

    The campaign fact sums each partner's ad group report to one row per
    campaign and day; the ad group fact does the same per ad group. The
    partner is the account. Days are UTC days, which The Trade Desk reports,
    paces and bills in whatever a campaign's own time zone, and so are the
    campaigns' scheduled start and end. Cost is the advertiser cost, what the
    advertiser is billed, in the advertiser's currency, the currency of its
    budgets; each row states it. The report's conversions are counted per
    tracking tag, none of them primary and none known to be purchases, so the
    facts publish no conversions or purchases. Dimensions take their rows from
    the entity snapshots, then the report, then the facts' keys, so every key
    a fact carries resolves. Every bound upstream's table must exist, or the
    view fails when it is created. Views are recreated on every run, so they
    follow the partners bound to them; a partner added to the organisation is
    absent until it is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={"ad_groups_stats": il.Relation("asset", "the_trade_desk.ad_groups_stats", many=True, optional=True)},
    )
    def fact_campaign_performance(self, ad_groups_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every partner, one row per campaign and day.

        Key: date, account_id, campaign_id.
        """
        rows = _performance(ad_groups_stats, "campaign_id")
        summed = rows.group_by(["date", "account_id", "campaign_id"]).aggregate(**_measures(rows))
        return summed.select("date", "account_id", "campaign_id", "impressions", "clicks", "cost", "currency")

    @ibis_view(
        schema=schemas.FactAdGroupPerformance,
        tags=["Fact"],
        relations={"ad_groups_stats": il.Relation("asset", "the_trade_desk.ad_groups_stats", many=True, optional=True)},
    )
    def fact_ad_group_performance(self, ad_groups_stats: list[il.Upstream]) -> ibis.Table:
        """Ad group performance of every partner, one row per ad group and day.

        Key: date, account_id, ad_group_id.
        """
        rows = _performance(ad_groups_stats, "ad_group_id")
        summed = rows.group_by(["date", "account_id", "ad_group_id"]).aggregate(
            campaign_id=rows["campaign_id"].max(), **_measures(rows)
        )
        return summed.select(
            "date", "account_id", "campaign_id", "ad_group_id", "impressions", "clicks", "cost", "currency"
        )

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={"ad_groups_stats": il.Relation("asset", "the_trade_desk.ad_groups_stats", many=True, optional=True)},
    )
    def dim_accounts(self, ad_groups_stats: list[il.Upstream]) -> ibis.Table:
        """One row per partner with its currency, from its latest report.

        No partner entity is collected, and the facts are built from the same
        reports but carry the advertiser's currency, not the partner's, so the
        report is the only tier: every partner a fact carries has its report.
        """
        rows = union(ad_groups_stats, "date", "partner_currency_code", account_id=_account)
        days = rows.group_by(["account_id", "date"]).aggregate(currency=rows["partner_currency_code"].max())
        accounts = days.select(
            account_id=days["account_id"],
            currency=days["currency"],
            timezone=ibis.literal(REPORT_TIMEZONE),
            as_of_date=days["date"],
        )
        return latest(accounts, "account_id", accounts["as_of_date"].desc())

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation("asset", "the_trade_desk.campaigns", many=True, optional=True),
            "ad_groups_stats": il.Relation("asset", "the_trade_desk.ad_groups_stats", many=True, optional=True),
        },
    )
    def dim_campaigns(
        self,
        campaigns: list[il.Upstream],
        ad_groups_stats: list[il.Upstream],
        fact_campaign_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per campaign, from its latest snapshot, else its latest report, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            budget = table["budget_total"]
            return table.select(
                campaign_id=table["id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=table["name"],
                status=table["status"],
                objective=table["primary_goal_type"],
                campaign_type=table["type"],
                budget=budget.cast("float64"),
                budget_type=ibis.ifelse(budget.notnull(), "lifetime", ibis.null("string")),
                start_date=local_date(table["start_date"].cast(INSTANT), REPORT_TIMEZONE),
                end_date=local_date(table["end_date"].cast(INSTANT), REPORT_TIMEZONE),
                created_at=table["created_at_utc"].cast(INSTANT),
                as_of_date=table["date"],
                advertiser_id=table["advertiser_id"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["date", "campaign_id"]).aggregate(
                campaign_name=table["campaign"].max(), advertiser_id=table["advertiser_id"].max()
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
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                advertiser_id=days["advertiser_id"],
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
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                advertiser_id=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in campaigns),
            *(from_report(upstream) for upstream in ad_groups_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={
            "ad_groups": il.Relation("asset", "the_trade_desk.ad_groups", many=True, optional=True),
            "ad_groups_stats": il.Relation("asset", "the_trade_desk.ad_groups_stats", many=True, optional=True),
        },
    )
    def dim_ad_groups(
        self,
        ad_groups: list[il.Upstream],
        ad_groups_stats: list[il.Upstream],
        fact_ad_group_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per ad group with its campaign, from its latest snapshot, else its latest report, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            budget = table["budget_in_advertiser_currency"]
            return table.select(
                ad_group_id=table["id"],
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=table["name"],
                status=table["status"],
                budget=budget.cast("float64"),
                budget_type=ibis.ifelse(budget.notnull(), "lifetime", ibis.null("string")),
                created_at=table["created_at"].cast(INSTANT),
                updated_at=table["last_updated"].cast(INSTANT),
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by(["date", "ad_group_id"]).aggregate(
                campaign_id=table["campaign_id"].max(), ad_group_name=table["ad_group"].max()
            )
            return days.select(
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=days["ad_group_name"],
                status=ibis.null("string"),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
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
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in ad_groups),
            *(from_report(upstream) for upstream in ad_groups_stats),
            from_fact(fact_ad_group_performance.data),
            distinct=False,
        )
        return latest(rows, "ad_group_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")
