"""Amazon Ads star schema: views over every Amazon Ads profile's tables."""

from __future__ import annotations

import ibis
import ibis.expr.types as ir
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest

from interloper_assets.amazon_ads.star_schema import schemas

AD_PRODUCTS = {
    "products_campaigns_stats": "sponsored_products",
    "brands_campaigns_stats": "sponsored_brands",
    "display_campaigns_stats": "sponsored_display",
    "brands_ad_groups_stats": "sponsored_brands",
    "display_ad_groups_stats": "sponsored_display",
    "products_campaigns": "sponsored_products",
    "brands_campaigns": "sponsored_brands",
    "display_campaigns": "sponsored_display",
    "products_ad_groups": "sponsored_products",
    "brands_ad_groups": "sponsored_brands",
    "display_ad_groups": "sponsored_display",
}
CAMPAIGN_ENTITIES = ["amazon_ads.products_campaigns", "amazon_ads.brands_campaigns", "amazon_ads.display_campaigns"]
AD_GROUP_ENTITIES = ["amazon_ads.products_ad_groups", "amazon_ads.brands_ad_groups", "amazon_ads.display_ad_groups"]
CAMPAIGN_REPORTS = [
    "amazon_ads.products_campaigns_stats",
    "amazon_ads.brands_campaigns_stats",
    "amazon_ads.display_campaigns_stats",
]
AD_GROUP_REPORTS = [
    "amazon_ads.products_campaigns_stats",
    "amazon_ads.brands_ad_groups_stats",
    "amazon_ads.display_ad_groups_stats",
]
AD_ENTITIES = ["amazon_ads.products_ads", "amazon_ads.brands_ads", "amazon_ads.display_ads"]
AD_REPORTS = [
    "amazon_ads.products_advertised_products_stats",
    "amazon_ads.brands_ads_stats",
    "amazon_ads.display_advertised_products_stats",
]
INSTANT = "timestamp('UTC')"
ENTITY, REPORT, FACT = 1, 2, 3


def _account(upstream: il.Upstream) -> str:
    """The profile an upstream's rows belong to; every view stamps it.

    The reports carry no profile column, so the profile is the source's
    discriminator, stamped onto every row.

    Args:
        upstream: A bound upstream of a profile's source.

    Returns:
        The profile id.

    Raises:
        ConfigError: If the upstream's asset belongs to no source, or its source has no discriminator.
    """
    source = upstream.asset.source
    profile = source.discriminator if source is not None else None
    if profile is None:
        raise ConfigError(f"Upstream '{upstream.asset.key}' belongs to no profile")
    return profile


def _lifecycle(upstream: il.Upstream) -> tuple[ir.Value, ir.Value]:
    """When an entity was created and last changed, from any ad product's entity table.

    Used by the campaign, ad group and ad dimensions. Each ad product names the
    two instants its own way: under the extended data on Sponsored Products
    and Brands, at the top level on Sponsored Display.

    Args:
        upstream: A bound campaigns or ad groups entity upstream of any ad product.

    Returns:
        The creation and last-update instants, in UTC.
    """
    table = upstream.data
    if upstream.asset.key.startswith("products_"):
        return table["extended_data_creation_date_time"], table["extended_data_last_update_date_time"]
    if upstream.asset.key.startswith("brands_"):
        return table["extended_data_creation_date"], table["extended_data_last_update_date"]
    return table["creation_date"], table["last_updated_date"]


@il.source(tags=["Analytics"], icon="icon:amazon", maturity="alpha", group="amazon_ads", dataset="amazon_ads")
class AmazonAdsStarSchema(il.Source):
    """Star schema over every Amazon Ads profile, as views beside the raw tables.

    The fact unions the campaign reports of every ad product (Sponsored
    Products, Sponsored Brands, Sponsored Display) across profiles, at
    campaign and day grain, with the purchases and sales each report
    attributes within 14 days of a click. The campaign, ad group and ad
    dimensions take each entity's own row where it is collected, else its
    latest report, and the campaigns are completed by the fact's keys; the
    account dimension comes from each profile's entity row, else the facts.
    Every bound upstream's table must exist, or the view fails when it is
    created. Views are recreated on every run, so they follow the profiles
    bound to them; a profile added to the organisation is absent until it
    is bound.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={"campaigns_stats": il.Relation("asset", CAMPAIGN_REPORTS, many=True, optional=True)},
    )
    def fact_campaign_performance(self, campaigns_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every profile and ad product, one row per campaign and day.

        Key: date, account_id, campaign_type, campaign_id.
        """

        def from_products(table: ibis.Table) -> ibis.Table:
            return table.select(
                date=table["date"],
                campaign_id=table["campaign_id"],
                impressions=table["impressions"],
                clicks=table["clicks"],
                cost=table["spend"],
                currency=table["campaign_budget_currency_code"],
                purchases=table["purchases_14d"],
                revenue=table["sales_14d"],
            )

        def from_report(table: ibis.Table) -> ibis.Table:
            return table.select(
                date=table["date"],
                campaign_id=table["campaign_id"],
                impressions=table["impressions"],
                clicks=table["clicks"],
                cost=table["cost"],
                currency=table["campaign_budget_currency_code"],
                purchases=table["purchases"],
                revenue=table["sales"],
            )

        if not campaigns_stats:
            raise ConfigError("no upstream bound: the fact needs at least one profile's campaign report")
        measures = {
            "products_campaigns_stats": from_products,
            "brands_campaigns_stats": from_report,
            "display_campaigns_stats": from_report,
        }
        rows = ibis.union(
            *(
                measures[upstream.asset.key](upstream.data).mutate(
                    account_id=ibis.literal(_account(upstream)),
                    campaign_type=ibis.literal(AD_PRODUCTS[upstream.asset.key]),
                )
                for upstream in campaigns_stats
            ),
            distinct=False,
        )
        # Sponsored Products reports one row per ad group, so the grain is restored by summing.
        summed = rows.group_by(["date", "account_id", "campaign_type", "campaign_id"]).aggregate(
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
            conversions=summed["purchases"],
            conversion_value=summed["revenue"],
            purchases=summed["purchases"],
            revenue=summed["revenue"],
            campaign_type=summed["campaign_type"],
        )

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={"profiles": il.Relation("asset", "amazon_ads.profiles", many=True, optional=True)},
    )
    def dim_accounts(self, profiles: list[il.Upstream], fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per profile with its account and marketplace, from its own entity row, else the facts."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            account = ibis.literal(_account(upstream))
            # the profiles endpoint lists every profile the credentials see, not only this source's
            own = table.filter(table["profile_id"] == account)
            return own.select(
                account_id=account,
                account_name=own["account_info_name"],
                currency=own["currency_code"],
                timezone=own["timezone"],
                as_of_date=own["date"],
                account_type=own["account_info_type"],
                marketplace_id=own["account_info_marketplace_string_id"],
                country_code=own["country_code"],
                priority=ibis.literal(ENTITY),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by([table["account_id"], table["date"]]).aggregate(currency=table["currency"].max())
            return days.select(
                account_id=days["account_id"],
                account_name=ibis.null("string"),
                currency=days["currency"],
                timezone=ibis.null("string"),
                as_of_date=days["date"],
                account_type=ibis.null("string"),
                marketplace_id=ibis.null("string"),
                country_code=ibis.null("string"),
                priority=ibis.literal(FACT),
            )

        rows = ibis.union(
            *(from_entity(upstream) for upstream in profiles),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "account_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation("asset", CAMPAIGN_ENTITIES, many=True, optional=True),
            "campaigns_stats": il.Relation("asset", CAMPAIGN_REPORTS, many=True, optional=True),
        },
    )
    def dim_campaigns(
        self,
        campaigns: list[il.Upstream],
        campaigns_stats: list[il.Upstream],
        fact_campaign_performance: il.Upstream,
    ) -> ibis.Table:
        """One row per campaign with its ad product, status, budget and schedule.

        Each campaign comes from its entity row, else its report, else the facts.
        """

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            # only Sponsored Products nests its budget
            products = upstream.asset.key == "products_campaigns"
            created_at, updated_at = _lifecycle(upstream)
            return table.select(
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=table["name"],
                status=table["state"].upper(),
                objective=ibis.null("string"),
                campaign_type=ibis.literal(AD_PRODUCTS[upstream.asset.key]),
                budget=table["budget_budget"] if products else table["budget"],
                budget_type=(table["budget_budget_type"] if products else table["budget_type"]).upper(),
                start_date=table["start_date"],
                end_date=table["end_date"],
                created_at=created_at,
                updated_at=updated_at,
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            # the Sponsored Display campaign report carries no budget type
            typed = "campaign_budget_type" in table.columns
            attributes = {
                "campaign_name": table["campaign_name"].max(),
                "status": table["campaign_status"].max(),
                "budget": table["campaign_budget_amount"].max(),
            }
            if typed:
                attributes["budget_type"] = table["campaign_budget_type"].max()
            days = table.group_by([table["date"], table["campaign_id"]]).aggregate(**attributes)
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                campaign_name=days["campaign_name"],
                status=days["status"].upper(),
                objective=ibis.null("string"),
                campaign_type=ibis.literal(AD_PRODUCTS[upstream.asset.key]),
                budget=days["budget"],
                budget_type=days["budget_type"].upper() if typed else ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                priority=ibis.literal(REPORT),
            )

        def from_fact(table: ibis.Table) -> ibis.Table:
            days = table.group_by([table["date"], table["campaign_id"]]).aggregate(
                account_id=table["account_id"].max(), campaign_type=table["campaign_type"].max()
            )
            return days.select(
                campaign_id=days["campaign_id"],
                account_id=days["account_id"],
                campaign_name=ibis.null("string"),
                status=ibis.null("string"),
                objective=ibis.null("string"),
                campaign_type=days["campaign_type"],
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
            *(from_report(upstream) for upstream in campaigns_stats),
            from_fact(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "campaign_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAdGroups,
        tags=["Dimension"],
        relations={
            "ad_groups": il.Relation("asset", AD_GROUP_ENTITIES, many=True, optional=True),
            "ad_groups_stats": il.Relation("asset", AD_GROUP_REPORTS, many=True, optional=True),
        },
    )
    def dim_ad_groups(self, ad_groups: list[il.Upstream], ad_groups_stats: list[il.Upstream]) -> ibis.Table:
        """One row per ad group with its name, campaign and status: its entity row, else its latest report."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            created_at, updated_at = _lifecycle(upstream)
            return table.select(
                ad_group_id=table["ad_group_id"],
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_group_name=table["name"],
                status=table["state"].upper(),
                budget=ibis.null("float64"),
                budget_type=ibis.null("string"),
                start_date=ibis.null("date"),
                end_date=ibis.null("date"),
                created_at=created_at,
                updated_at=updated_at,
                as_of_date=table["date"],
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            days = table.group_by([table["date"], table["ad_group_id"]]).aggregate(
                ad_group_name=table["ad_group_name"].max(), campaign_id=table["campaign_id"].max()
            )
            return days.select(
                ad_group_id=days["ad_group_id"],
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
                as_of_date=days["date"],
                priority=ibis.literal(REPORT),
            )

        if not ad_groups and not ad_groups_stats:
            raise ConfigError("no upstream bound: the ad group dimension needs at least one profile's ad groups")
        rows = ibis.union(
            *(from_entity(upstream) for upstream in ad_groups),
            *(from_report(upstream) for upstream in ad_groups_stats),
            distinct=False,
        )
        return latest(rows, "ad_group_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")

    @ibis_view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={
            "ads": il.Relation("asset", AD_ENTITIES, many=True, optional=True),
            "ads_stats": il.Relation("asset", AD_REPORTS, many=True, optional=True),
        },
    )
    def dim_ads(self, ads: list[il.Upstream], ads_stats: list[il.Upstream]) -> ibis.Table:
        """One row per ad with its ad group, campaign and advertised product: its entity row, else its latest report."""

        def from_entity(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            created_at, updated_at = _lifecycle(upstream)
            none = ibis.null("string")
            if upstream.asset.key == "brands_ads":
                ad_name, asin, sku = table["name"], none, none
            elif upstream.asset.key == "products_ads":
                ad_name, asin, sku = none, table["asin"], table["sku"]
            else:
                ad_name, asin, sku = table["ad_name"], table["asin"], table["sku"]
            return table.select(
                ad_id=table["ad_id"],
                ad_group_id=table["ad_group_id"],
                campaign_id=table["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=ad_name,
                status=table["state"].upper(),
                created_at=created_at,
                updated_at=updated_at,
                as_of_date=table["date"],
                asin=asin,
                sku=sku,
                priority=ibis.literal(ENTITY),
            )

        def from_report(upstream: il.Upstream) -> ibis.Table:
            table = upstream.data
            none = ibis.null("string")
            if upstream.asset.key == "products_advertised_products_stats":
                asin, sku = table["advertised_asin"], table["advertised_sku"]
            elif upstream.asset.key == "display_advertised_products_stats":
                asin, sku = table["promoted_asin"], none
            else:
                asin, sku = none, none
            table = table.select(
                "date", "ad_id", "ad_group_id", "campaign_id", advertised_asin=asin, advertised_sku=sku
            )
            days = table.group_by([table["date"], table["ad_id"]]).aggregate(
                ad_group_id=table["ad_group_id"].max(),
                campaign_id=table["campaign_id"].max(),
                asin=table["advertised_asin"].max(),
                sku=table["advertised_sku"].max(),
            )
            return days.select(
                ad_id=days["ad_id"],
                ad_group_id=days["ad_group_id"],
                campaign_id=days["campaign_id"],
                account_id=ibis.literal(_account(upstream)),
                ad_name=ibis.null("string"),
                status=ibis.null("string"),
                created_at=ibis.null(INSTANT),
                updated_at=ibis.null(INSTANT),
                as_of_date=days["date"],
                asin=days["asin"],
                sku=days["sku"],
                priority=ibis.literal(REPORT),
            )

        if not ads and not ads_stats:
            raise ConfigError("no upstream bound: the ad dimension needs at least one profile's ads")
        rows = ibis.union(
            *(from_entity(upstream) for upstream in ads),
            *(from_report(upstream) for upstream in ads_stats),
            distinct=False,
        )
        return latest(rows, "ad_id", rows["priority"].asc(), rows["as_of_date"].desc()).drop("priority")
