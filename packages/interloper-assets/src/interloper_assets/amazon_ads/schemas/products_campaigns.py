import datetime

from interloper.schema import Schema
from pydantic import Field


class ProductsCampaigns(Schema):
    """Products campaign performance metrics including clicks, impressions, sales, and purchases across time windows."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    ad_status: str | None = Field(..., description="The status of the ad")
    attributed_sales_same_sku_14d: float | None = Field(
        ..., description="The attributed sales for the same SKU within 14 days"
    )
    attributed_sales_same_sku_1d: float | None = Field(
        ..., description="The attributed sales for the same SKU within 1 day"
    )
    attributed_sales_same_sku_30d: float | None = Field(
        ..., description="The attributed sales for the same SKU within 30 days"
    )
    attributed_sales_same_sku_7d: float | None = Field(
        ..., description="The attributed sales for the same SKU within 7 days"
    )
    campaign_applicable_budget_rule_name: float | None = Field(
        ..., description="The name of the applicable budget rule for the campaign"
    )
    campaign_applicable_budget_ruleid: int | None = Field(
        ..., description="The ID of the applicable budget rule for the campaign"
    )
    campaign_bidding_strategy: str | None = Field(..., description="The bidding strategy of the campaign")
    campaign_budget_amount: float | None = Field(..., description="The amount of campaign budget")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_rule_based_budget_amount: float | None = Field(
        ..., description="The budget amount based on campaign rules"
    )
    campaign_status: str | None = Field(..., description="The status of the campaign")
    click_through_rate: float | None = Field(..., description="The click-through rate")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost")
    cost_per_click: float | None = Field(..., description="The cost per click")
    date: datetime.date | None = Field(..., description="The date of the campaign")
    impressions: float | None = Field(..., description="The number of impressions")
    kindle_edition_normalized_pages_read_14d: float | None = Field(
        ..., description="The number of Kindle edition normalized pages read within 14 days"
    )
    kindle_edition_normalized_pages_royalties_14d: float | None = Field(
        ..., description="The royalties for Kindle edition normalized pages within 14 days"
    )
    purchases_14d: float | None = Field(..., description="The purchases within 14 days")
    purchases_1d: float | None = Field(..., description="The purchases within 1 day")
    purchases_30d: float | None = Field(..., description="The purchases within 30 days")
    purchases_7d: float | None = Field(..., description="The purchases within 7 days")
    purchases_same_sku_14d: float | None = Field(..., description="The purchases for the same SKU within 14 days")
    purchases_same_sku_1d: float | None = Field(..., description="The purchases for the same SKU within 1 day")
    purchases_same_sku_30d: float | None = Field(..., description="The purchases for the same SKU within 30 days")
    purchases_same_sku_7d: float | None = Field(..., description="The purchases for the same SKU within 7 days")
    sales_14d: float | None = Field(..., description="The sales within 14 days")
    sales_1d: float | None = Field(..., description="The sales within 1 day")
    sales_30d: float | None = Field(..., description="The sales within 30 days")
    sales_7d: float | None = Field(..., description="The sales within 7 days")
    spend: float | None = Field(..., description="The amount spent")
    units_sold_clicks_14d: float | None = Field(..., description="The units sold through clicks within 14 days")
    units_sold_clicks_1d: float | None = Field(..., description="The units sold through clicks within 1 day")
    units_sold_clicks_30d: float | None = Field(..., description="The units sold through clicks within 30 days")
    units_sold_clicks_7d: int | None = Field(..., description="The units sold through clicks within 7 days")
    units_sold_same_sku_14d: float | None = Field(..., description="The units sold for the same SKU within 14 days")
    units_sold_same_sku_1d: float | None = Field(..., description="The units sold for the same SKU within 1 day")
    units_sold_same_sku_30d: float | None = Field(..., description="The units sold for the same SKU within 30 days")
    units_sold_same_sku_7d: float | None = Field(..., description="The units sold for the same SKU within 7 days")
