"""Amazon Ads star schema: views over every Amazon Ads profile's tables."""

from __future__ import annotations

import ibis
import interloper as il
from interloper.errors import ConfigError
from interloper_sql import ibis_view, latest

from interloper_assets.amazon_ads.star_schema import schemas

AD_PRODUCTS = {
    "products_campaigns_stats": "sponsored_products",
    "brands_campaigns_stats": "sponsored_brands",
    "display_campaigns_stats": "sponsored_display",
}


def _profile(upstream: il.Upstream) -> str:
    """The profile an upstream's rows belong to; shared by the fact and the account dimension.

    The campaign reports carry no profile column, so the profile is the
    source's discriminator, stamped onto every row.

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


@il.source(tags=["Analytics"], icon="icon:amazon", maturity="alpha", group="amazon_ads", dataset="amazon_ads")
class AmazonAdsStarSchema(il.Source):
    """Star schema over every Amazon Ads profile, as views beside the raw tables.

    The fact unions the campaign reports of every ad product (Sponsored
    Products, Sponsored Brands, Sponsored Display) across profiles, at
    campaign and day grain, with the purchases and sales each report
    attributes within 14 days of a click. Each campaign dimension row comes
    from the facts; each profile from its latest entity row, else the facts.
    Every bound upstream's table must exist, or the view fails when it is
    created. Views are recreated on every run, so they follow the profiles
    bound to them.
    """

    @ibis_view(
        schema=schemas.FactCampaignPerformance,
        tags=["Fact"],
        relations={
            "campaigns_stats": il.Relation(
                "asset",
                [
                    "amazon_ads.products_campaigns_stats",
                    "amazon_ads.brands_campaigns_stats",
                    "amazon_ads.display_campaigns_stats",
                ],
                many=True,
                optional=True,
            )
        },
    )
    def fact_campaign_performance(self, campaigns_stats: list[il.Upstream]) -> ibis.Table:
        """Campaign performance of every profile and ad product, one row per campaign and day."""

        def from_products(table: ibis.Table) -> ibis.Table:
            return table.select(
                date=table["date"],
                campaign_id=table["campaign_id"].cast("string"),
                campaign_name=table["campaign_name"],
                campaign_status=table["campaign_status"],
                campaign_budget_amount=table["campaign_budget_amount"],
                campaign_budget_currency_code=table["campaign_budget_currency_code"],
                impressions=table["impressions"],
                clicks=table["clicks"],
                cost=table["spend"],
                purchases=table["purchases_14d"],
                sales=table["sales_14d"],
            )

        def from_report(table: ibis.Table) -> ibis.Table:
            return table.select(
                date=table["date"],
                campaign_id=table["campaign_id"].cast("string"),
                campaign_name=table["campaign_name"],
                campaign_status=table["campaign_status"],
                campaign_budget_amount=table["campaign_budget_amount"],
                campaign_budget_currency_code=table["campaign_budget_currency_code"],
                impressions=table["impressions"],
                clicks=table["clicks"],
                cost=table["cost"],
                purchases=table["purchases"],
                sales=table["sales"],
            )

        if not campaigns_stats:
            raise ConfigError("no upstream bound: the fact needs at least one profile's campaign report")
        candidates = {
            "products_campaigns_stats": from_products,
            "brands_campaigns_stats": from_report,
            "display_campaigns_stats": from_report,
        }
        rows = ibis.union(
            *(
                candidates[upstream.asset.key](upstream.data).mutate(
                    profile_id=ibis.literal(_profile(upstream)),
                    ad_product=ibis.literal(AD_PRODUCTS[upstream.asset.key]),
                )
                for upstream in campaigns_stats
            ),
            distinct=False,
        )
        # Sponsored Products reports one row per ad group, so the grain is restored by summing.
        return rows.group_by(["date", "profile_id", "ad_product", "campaign_id"]).aggregate(
            campaign_name=rows["campaign_name"].max(),
            campaign_status=rows["campaign_status"].max(),
            campaign_budget_amount=rows["campaign_budget_amount"].max(),
            campaign_budget_currency_code=rows["campaign_budget_currency_code"].max(),
            impressions=rows["impressions"].sum(),
            clicks=rows["clicks"].sum(),
            cost=rows["cost"].sum(),
            purchases=rows["purchases"].sum(),
            sales=rows["sales"].sum(),
        )

    @ibis_view(schema=schemas.DimCampaigns, tags=["Dimension"])
    def dim_campaigns(self, fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per campaign with its ad product and profile, from the facts."""
        rows = fact_campaign_performance.data.select(
            "campaign_id",
            "campaign_name",
            "ad_product",
            "profile_id",
            "campaign_status",
            "campaign_budget_amount",
            "campaign_budget_currency_code",
            "date",
        )
        return latest(rows, "campaign_id", rows["date"].desc())

    @ibis_view(
        schema=schemas.DimAccounts,
        tags=["Dimension"],
        relations={"profiles": il.Relation("asset", "amazon_ads.profiles", many=True, optional=True)},
    )
    def dim_accounts(self, profiles: list[il.Upstream], fact_campaign_performance: il.Upstream) -> ibis.Table:
        """One row per profile with its account and marketplace, from its latest entity row, else the facts."""

        def from_entity(table: ibis.Table) -> ibis.Table:
            return table.select(
                profile_id=table["profile_id"].cast("string"),
                account_name=table["account_info_name"],
                account_type=table["account_info_type"],
                marketplace_id=table["account_info_marketplace_string_id"],
                country_code=table["country_code"],
                currency_code=table["currency_code"],
                timezone=table["timezone"],
                date=table["date"],
                priority=ibis.literal(1),
            )

        def from_facts(table: ibis.Table) -> ibis.Table:
            return table.select(
                profile_id=table["profile_id"],
                account_name=ibis.null("string"),
                account_type=ibis.null("string"),
                marketplace_id=ibis.null("string"),
                country_code=ibis.null("string"),
                currency_code=table["campaign_budget_currency_code"],
                timezone=ibis.null("string"),
                date=table["date"],
                priority=ibis.literal(2),
            )

        rows = ibis.union(
            *(from_entity(upstream.data) for upstream in profiles),
            from_facts(fact_campaign_performance.data),
            distinct=False,
        )
        return latest(rows, "profile_id", rows["priority"].asc(), rows["date"].desc()).drop("priority")
