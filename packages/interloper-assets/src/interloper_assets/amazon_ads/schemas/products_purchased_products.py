import datetime

from interloper.schema import Schema
from pydantic import Field


class ProductsPurchasedProducts(Schema):
    """Products purchased product metrics including sales, purchases, and units sold across time windows."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    advertised_asin: str | None = Field(..., description="The ASIN of the advertised product")
    advertised_sku: str | None = Field(..., description="The SKU of the advertised product")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    date: datetime.date | None = Field(..., description="The date of the purchase")
    keyword: str | None = Field(..., description="The keyword")
    keyword_id: int | None = Field(..., description="The ID of the keyword")
    keyword_type: str | None = Field(..., description="The type of keyword")
    kindle_edition_normalized_pages_read_14d: float | None = Field(
        ..., description="The number of Kindle Edition Normalized Pages read in the last 14 days"
    )
    kindle_edition_normalized_pages_royalties_14d: float | None = Field(
        ..., description="The royalties for Kindle Edition Normalized Pages in the last 14 days"
    )
    match_type: str | None = Field(..., description="The type of match")
    portfolio_id: int | None = Field(..., description="The ID of the portfolio")
    purchased_asin: str | None = Field(..., description="The ASIN of the purchased product")
    purchases_14d: float | None = Field(..., description="The number of purchases in the last 14 days")
    purchases_1d: float | None = Field(..., description="The number of purchases in the last 1 day")
    purchases_30d: float | None = Field(..., description="The number of purchases in the last 30 days")
    purchases_7d: float | None = Field(..., description="The number of purchases in the last 7 days")
    purchases_other_sku_14d: float | None = Field(
        ..., description="The number of purchases for other SKUs in the last 14 days"
    )
    purchases_other_sku_1d: float | None = Field(
        ..., description="The number of purchases for other SKUs in the last 1 day"
    )
    purchases_other_sku_30d: float | None = Field(
        ..., description="The number of purchases for other SKUs in the last 30 days"
    )
    purchases_other_sku_7d: float | None = Field(
        ..., description="The number of purchases for other SKUs in the last 7 days"
    )
    sales_14d: float | None = Field(..., description="The sales in the last 14 days")
    sales_1d: float | None = Field(..., description="The sales in the last 1 day")
    sales_30d: float | None = Field(..., description="The sales in the last 30 days")
    sales_7d: float | None = Field(..., description="The sales in the last 7 days")
    sales_other_sku_14d: float | None = Field(..., description="The sales for other SKUs in the last 14 days")
    sales_other_sku_1d: float | None = Field(..., description="The sales for other SKUs in the last 1 day")
    sales_other_sku_30d: float | None = Field(..., description="The sales for other SKUs in the last 30 days")
    sales_other_sku_7d: float | None = Field(..., description="The sales for other SKUs in the last 7 days")
    units_sold_clicks_14d: float | None = Field(
        ..., description="The number of units sold from clicks in the last 14 days"
    )
    units_sold_clicks_1d: float | None = Field(
        ..., description="The number of units sold from clicks in the last 1 day"
    )
    units_sold_clicks_30d: float | None = Field(
        ..., description="The number of units sold from clicks in the last 30 days"
    )
    units_sold_clicks_7d: float | None = Field(
        ..., description="The number of units sold from clicks in the last 7 days"
    )
    units_sold_other_sku_14d: float | None = Field(
        ..., description="The number of units sold for other SKUs in the last 14 days"
    )
    units_sold_other_sku_1d: float | None = Field(
        ..., description="The number of units sold for other SKUs in the last 1 day"
    )
    units_sold_other_sku_30d: float | None = Field(
        ..., description="The number of units sold for other SKUs in the last 30 days"
    )
    units_sold_other_sku_7d: float | None = Field(
        ..., description="The number of units sold for other SKUs in the last 7 days"
    )
