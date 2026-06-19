import datetime

from interloper.schema import Schema
from pydantic import Field


class ProductsTargetingStats(Schema):
    """Products targeting performance metrics including click-through rate, sales, purchases, and targeting criteria."""

    acos_clicks_14d: float | None = Field(..., description="The advertising cost of sales for clicks within 14 days")
    acos_clicks_7d: float | None = Field(..., description="The advertising cost of sales for clicks within 7 days")
    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
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
    campaign_budget_amount: float | None = Field(..., description="The budget amount for the campaign")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code for the campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of the campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_status: str | None = Field(..., description="The status of the campaign")
    click_through_rate: float | None = Field(..., description="The click-through rate")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost of the ad")
    cost_per_click: float | None = Field(..., description="The cost per click")
    date: datetime.date | None = Field(..., description="The date of the record")
    impressions: float | None = Field(..., description="The number of impressions")
    keyword: str | None = Field(..., description="The keyword used for targeting")
    keyword_bid: float | None = Field(..., description="The bid for the keyword")
    keyword_id: int | None = Field(..., description="The ID of the keyword")
    keyword_type: str | None = Field(..., description="The type of the keyword")
    kindle_edition_normalized_pages_read_14d: float | None = Field(
        ..., description="The pages read for Kindle Edition Normalized Pages within 14 days"
    )
    kindle_edition_normalized_pages_royalties_14d: float | None = Field(
        ..., description="The royalties for Kindle Edition Normalized Pages within 14 days"
    )
    match_type: str | None = Field(..., description="The match type of the keyword")
    portfolio_id: int | None = Field(..., description="The ID of the portfolio")
    purchases_14d: float | None = Field(..., description="The purchases within 14 days")
    purchases_1d: float | None = Field(..., description="The purchases within 1 day")
    purchases_30d: float | None = Field(..., description="The purchases within 30 days")
    purchases_7d: float | None = Field(..., description="The purchases within 7 days")
    purchases_same_sku_14d: float | None = Field(..., description="The purchases for the same SKU within 14 days")
    purchases_same_sku_1d: float | None = Field(..., description="The purchases for the same SKU within 1 day")
    purchases_same_sku_30d: float | None = Field(..., description="The purchases for the same SKU within 30 days")
    purchases_same_sku_7d: float | None = Field(..., description="The purchases for the same SKU within 7 days")
    roas_clicks_14d: float | None = Field(..., description="The return on ad spend for clicks within 14 days")
    roas_clicks_7d: float | None = Field(..., description="The return on ad spend for clicks within 7 days")
    sales_14d: float | None = Field(..., description="The sales within 14 days")
    sales_1d: float | None = Field(..., description="The sales within 1 day")
    sales_30d: float | None = Field(..., description="The sales within 30 days")
    sales_7d: float | None = Field(..., description="The sales within 7 days")
    sales_other_sku_7d: float | None = Field(..., description="The sales for other SKUs within 7 days")
    targeting: str | None = Field(..., description="The targeting used for the ad")
    top_of_search_impression_share: float | None = Field(..., description="The impression share for top of search")
    units_sold_clicks_14d: float | None = Field(..., description="The units sold for clicks within 14 days")
    units_sold_clicks_1d: float | None = Field(..., description="The units sold for clicks within 1 day")
    units_sold_clicks_30d: float | None = Field(..., description="The units sold for clicks within 30 days")
    units_sold_clicks_7d: float | None = Field(..., description="The units sold for clicks within 7 days")
    units_sold_other_sku_7d: float | None = Field(..., description="The units sold for other SKUs within 7 days")
    units_sold_same_sku_14d: float | None = Field(..., description="The units sold for the same SKU within 14 days")
    units_sold_same_sku_1d: float | None = Field(..., description="The units sold for the same SKU within 1 day")
    units_sold_same_sku_30d: float | None = Field(..., description="The units sold for the same SKU within 30 days")
    units_sold_same_sku_7d: float | None = Field(..., description="The units sold for the same SKU within 7 days")
