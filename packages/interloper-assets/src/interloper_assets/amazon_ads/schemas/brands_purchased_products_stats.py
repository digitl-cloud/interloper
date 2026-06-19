import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsPurchasedProductsStats(Schema):
    """Brands purchased product metrics including purchases, sales, and units sold with new-to-brand breakdowns."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    attribution_type: str | None = Field(..., description="The type of attribution")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    date: datetime.date | None = Field(..., description="The date of the record")
    new_to_brand_purchases_14d: float | None = Field(
        ..., description="The number of purchases made by new customers in the last 14 days"
    )
    new_to_brand_purchases_percentage_14d: float | None = Field(
        ..., description="The percentage of purchases made by new customers in the last 14 days"
    )
    new_to_brand_sales_14d: float | None = Field(
        ..., description="The total sales made by new customers in the last 14 days"
    )
    new_to_brand_sales_percentage_14d: float | None = Field(
        ..., description="The percentage of sales made by new customers in the last 14 days"
    )
    new_to_brand_units_sold_14d: float | None = Field(
        ..., description="The number of units sold to new customers in the last 14 days"
    )
    new_to_brand_units_sold_percentage_14d: float | None = Field(
        ..., description="The percentage of units sold to new customers in the last 14 days"
    )
    orders_14d: float | None = Field(..., description="The total number of orders in the last 14 days")
    product_category: str | None = Field(..., description="The category of the product")
    product_name: str | None = Field(..., description="The name of the product")
    purchased_asin: str | None = Field(..., description="The ASIN of the purchased product")
    sales_14d: float | None = Field(..., description="The total sales in the last 14 days")
    units_sold_14d: float | None = Field(..., description="The total number of units sold in the last 14 days")
