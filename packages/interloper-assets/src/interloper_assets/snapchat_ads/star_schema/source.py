"""Snapchat Ads star schema: views over every Snapchat Ads account's tables."""

from __future__ import annotations

import ibis
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest, local_date, union

from interloper_assets.snapchat_ads.star_schema import schemas

REPORT_COLUMNS = ("date", "id", "impressions", "swipes", "spend", "conversion_purchases", "conversion_purchases_value")
CAMPAIGN_ENTITY_COLUMNS = (
    "date",
    "id",
    "name",
    "status",
    "objective",
    "buy_model",
    "lifetime_spend_cap_micro",
    "start_time",
    "end_time",
    "created_at",
    "updated_at",
)
AD_SQUAD_ENTITY_COLUMNS = (
    "date",
    "id",
    "campaign_id",
    "name",
    "status",
    "lifetime_budget_micro",
    "start_time",
    "end_time",
    "created_at",
    "updated_at",
)
MICRO = 1_000_000
INSTANT = "timestamp('UTC')"
ENTITY, FACT = 1, 2


def _account(upstream: il.Upstream) -> str:
    """The account an upstream's rows belong to; every view stamps it.

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


def _performance(reports: list[il.Upstream], ad_accounts: list[il.Upstream]) -> ibis.Table:
    """Sum a report to one row per object and day, in currency units, with the account's currency.

    Used by both facts: Snapchat reports amounts in micro-units of the
    account currency and does not say which currency, so each row takes the
    currency of its account's latest snapshot, NULL where none is collected.

    Args:
        reports: The bound campaign or ad reports, one per account.
        ad_accounts: The bound ad account snapshots, one per account.

    Returns:
        date, account_id, id and the summed measures, with currency.
    """
    rows = union(reports, *REPORT_COLUMNS, account_id=_account)
    summed = rows.group_by(["date", "account_id", "id"]).aggregate(
        impressions=rows["impressions"].sum(),
        clicks=rows["swipes"].sum(),
        cost=rows["spend"].sum() / MICRO,
        purchases=rows["conversion_purchases"].sum(),
        revenue=rows["conversion_purchases_value"].sum() / MICRO,
    )
    if not ad_accounts:
        return summed.mutate(currency=ibis.null("string"))
    snapshots = union(ad_accounts, "date", "currency", account_id=_account)
    currencies = latest(snapshots, "account_id", snapshots["date"].desc()).select("account_id", "currency")
    return summed.left_join(currencies, "account_id")


def _timezones(dim_accounts: il.Upstream) -> ibis.Table:
    """Each account's timezone, to read a schedule's UTC instants as the account's days.

    Args:
        dim_accounts: The account dimension.

    Returns:
        account_id and timezone, one row per account.
    """
    return dim_accounts.data.select(zone_account_id="account_id", timezone="timezone")


@il.source(tags=["Analytics"], icon="mdi:snapchat", maturity="alpha", group="snapchat_ads", dataset="snapchat_ads")
class SnapchatAdsStarSchema(il.Source):
    """Star schema over every Snapchat Ads account, as views beside the raw tables.

    The campaign fact unions each account's campaign report, one row per
    campaign and day; the ad fact does the same at the ad report's grain.
    Snapchat reports amounts in micro-units and swipe-ups as its clicks; the
    views give amounts in the account currency, taken from the account
    snapshot. Dimensions take their rows from the entity snapshots and fall
    back to the facts' keys, so every key a fact carries resolves; schedules
    are read in the account's own timezone. Every bound upstream's table must
    exist, or the view fails when it is created. Views are recreated on every
    run, so they follow the accounts bound to them; an account added to the
    organisation is absent until it is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={
            "campaigns_stats": il.Relation("asset", "snapchat_ads.campaigns_stats", many=True, optional=True),
            "ad_accounts": il.Relation("asset", "snapchat_ads.ad_accounts", many=True, optional=True),
        },
    )
    def fact_campaign_performance(
        self, campaigns_stats: list[il.Upstream], ad_accounts: list[il.Upstream]
    ) -> ibis.Table:
        """Campaign performance of every account, one row per campaign and day.

        Key: date, account_id, campaign_id.
        """
        rows = _performance(campaigns_stats, ad_accounts)
        return rows.select(
            "date",
            "account_id",
            campaign_id=rows["id"],
            impressions=rows["impressions"],
            clicks=rows["clicks"],
            cost=rows["cost"],
            currency=rows["currency"],
            purchases=rows["purchases"],
            revenue=rows["revenue"],
        )

    @ibis_view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={
            "ads_stats": il.Relation("asset", "snapchat_ads.ads_stats", many=True, optional=True),
            "ad_accounts": il.Relation("asset", "snapchat_ads.ad_accounts", many=True, optional=True),
        },
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream], ad_accounts: list[il.Upstream]) -> ibis.Table:
        """Ad performance of every account, one row per ad and day.

        Key: date, account_id, ad_id.
        """
        rows = _performance(ads_stats, ad_accounts)
        return rows.select(
            "date",
            "account_id",
            ad_id=rows["id"],
            impressions=rows["impressions"],
            clicks=rows["clicks"],
            cost=rows["cost"],
            currency=rows["currency"],
            purchases=rows["purchases"],
            revenue=rows["revenue"],
        )

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={"ad_accounts": il.Relation("asset", "snapchat_ads.ad_accounts", many=True, optional=True)},
    )
    def dim_accounts(self, ad_accounts: list[il.Upstream], fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per ad account with its name, currency and timezone: its latest snapshot, else the facts."""

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
            *(from_entity(upstream) for upstream in ad_accounts),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "account_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={"campaigns": il.Relation("asset", "snapchat_ads.campaigns", many=True, optional=True)},
    )
    def dim_campaigns(
        self, campaigns: list[il.Upstream], fact_campaign_performance: il.Upstream, dim_accounts: il.Upstream
    ) -> ibis.Table:
        """One row per campaign, from its latest snapshot, else the facts."""
        timezones = _timezones(dim_accounts)

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = union([upstream], *CAMPAIGN_ENTITY_COLUMNS, account_id=_account)
            table = table.left_join(timezones, table["account_id"] == timezones["zone_account_id"])
            cap = table["lifetime_spend_cap_micro"]
            return table.select(
                campaign_id=table["id"],
                account_id=table["account_id"],
                campaign_name=table["name"],
                status=table["status"],
                objective=table["objective"],
                campaign_type=table["buy_model"],
                budget=cap / MICRO,
                budget_type=ibis.ifelse(cap.notnull(), "lifetime", ibis.null("string")),
                start_date=local_date(table["start_time"], table["timezone"]),
                end_date=local_date(table["end_time"], table["timezone"]),
                created_at=table["created_at"],
                updated_at=table["updated_at"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
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
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in campaigns),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={"ad_squads": il.Relation("asset", "snapchat_ads.ad_squads", many=True, optional=True)},
    )
    def dim_ad_groups(self, ad_squads: list[il.Upstream], dim_accounts: il.Upstream) -> ibis.Table:
        """One row per ad squad, from its latest snapshot.

        No Snapchat report carries the ad squad, so the snapshots are the only
        source: the view needs an ad squads table bound.
        """
        timezones = _timezones(dim_accounts)
        rows = union(ad_squads, *AD_SQUAD_ENTITY_COLUMNS, account_id=_account)
        rows = rows.left_join(timezones, rows["account_id"] == timezones["zone_account_id"])
        budget = rows["lifetime_budget_micro"]
        squads = rows.select(
            ad_group_id=rows["id"],
            campaign_id=rows["campaign_id"],
            account_id=rows["account_id"],
            ad_group_name=rows["name"],
            status=rows["status"],
            budget=budget / MICRO,
            budget_type=ibis.ifelse(budget.notnull(), "lifetime", ibis.null("string")),
            start_date=local_date(rows["start_time"], rows["timezone"]),
            end_date=local_date(rows["end_time"], rows["timezone"]),
            created_at=rows["created_at"],
            updated_at=rows["updated_at"],
            as_of_date=rows["date"],
        )
        return latest(squads, "ad_group_id", squads["as_of_date"].desc())

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={"ads": il.Relation("asset", "snapchat_ads.ads", many=True, optional=True)},
    )
    def dim_ads(
        self, ads: list[il.Upstream], fact_ad_performance: il.Upstream, dim_ad_groups: il.Upstream
    ) -> ibis.Table:
        """One row per ad with its ad squad and campaign, from its latest snapshot, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            return table.select(
                ad_id=table["id"],
                ad_group_id=table["ad_squad_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=table["name"],
                status=table["status"],
                created_at=table["created_at"],
                updated_at=table["updated_at"],
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by(["date", "ad_id"]).aggregate(account_id=table["account_id"].max())
            return days.select(
                ad_id=days["ad_id"],
                ad_group_id=ibis.null("string"),
                account_id=days["account_id"],
                ad_name=ibis.null("string"),
                status=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in ads),
            from_fact(fact_ad_performance.data),
            distinct=False,
        )
        kept = latest(rows, "ad_id", rows["priority"].asc(), rows["as_of_date"].desc())
        squads = dim_ad_groups.data.select(squad_id="ad_group_id", campaign_id="campaign_id")
        kept = kept.left_join(squads, kept["ad_group_id"] == squads["squad_id"])
        return kept.select(
            "ad_id",
            "ad_group_id",
            "campaign_id",
            "account_id",
            "ad_name",
            "status",
            "created_at",
            "updated_at",
            "as_of_date",
        )
