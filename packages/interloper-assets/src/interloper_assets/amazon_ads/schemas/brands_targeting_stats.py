import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsTargetingStats(Schema):
    """Brands targeting performance metrics including clicks, impressions, purchases, and targeting expression details."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    ad_keyword_status: str | None = Field(..., description="The status of the ad keyword")
    add_to_cart: float | None = Field(..., description="The number of add to cart")
    add_to_cart_clicks: float | None = Field(..., description="The number of clicks for add to cart")
    add_to_cart_rate: float | None = Field(..., description="The rate of add to cart")
    branded_searches: float | None = Field(..., description="The number of branded searches")
    branded_searches_clicks: float | None = Field(..., description="The number of clicks for branded searches")
    campaign_budget_amount: float | None = Field(..., description="The amount of campaign budget")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_status: str | None = Field(..., description="The status of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost")
    cost_type: str | None = Field(..., description="The type of cost")
    date: datetime.date | None = Field(..., description="The date of the event")
    detail_page_views: float | None = Field(..., description="The number of detail page views")
    detail_page_views_clicks: float | None = Field(..., description="The number of clicks for detail page views")
    ecp_add_to_cart: float | None = Field(..., description="The eCP add to cart")
    impressions: float | None = Field(..., description="The number of impressions")
    keyword_bid: float | None = Field(..., description="The bid for the keyword")
    keyword_id: int | None = Field(..., description="The ID of the keyword")
    keyword_text: str | None = Field(..., description="The text of the keyword")
    keyword_type: str | None = Field(..., description="The type of keyword")
    match_type: str | None = Field(..., description="The type of match")
    new_to_brand_detail_page_view_rate: float | None = Field(
        ..., description="The rate of new-to-brand detail page views"
    )
    new_to_brand_detail_page_views: float | None = Field(
        ..., description="The number of new-to-brand detail page views"
    )
    new_to_brand_detail_page_views_clicks: float | None = Field(
        ..., description="The number of clicks for new-to-brand detail page views"
    )
    new_to_brand_ecp_detail_page_view: float | None = Field(
        ..., description="The ECP detail page views for new-to-brand"
    )
    new_to_brand_purchases: float | None = Field(..., description="The number of new-to-brand purchases")
    new_to_brand_purchases_clicks: float | None = Field(
        ..., description="The number of clicks for new-to-brand purchases"
    )
    new_to_brand_purchases_percentage: float | None = Field(..., description="The percentage of new-to-brand purchases")
    new_to_brand_purchases_rate: float | None = Field(..., description="The rate of new-to-brand purchases")
    new_to_brand_sales: float | None = Field(..., description="The number of new-to-brand sales")
    new_to_brand_sales_clicks: float | None = Field(..., description="The number of clicks for new-to-brand sales")
    new_to_brand_sales_percentage: float | None = Field(..., description="The percentage of new-to-brand sales")
    new_to_brand_units_sold: float | None = Field(..., description="The number of new-to-brand units sold")
    new_to_brand_units_sold_clicks: float | None = Field(
        ..., description="The number of clicks for new-to-brand units sold"
    )
    new_to_brand_units_sold_percentage: float | None = Field(
        ..., description="The percentage of new-to-brand units sold"
    )
    purchases: float | None = Field(..., description="The number of purchases")
    purchases_clicks: float | None = Field(..., description="The number of clicks for purchases")
    purchases_promoted: float | None = Field(..., description="The number of promoted purchases")
    sales: float | None = Field(..., description="The number of sales")
    sales_clicks: float | None = Field(..., description="The number of clicks for sales")
    sales_promoted: float | None = Field(..., description="The number of promoted sales")
    targeting_expression: str | None = Field(..., description="The expression of the targeting")
    targeting_id: int | None = Field(..., description="The ID of the targeting")
    targeting_text: str | None = Field(..., description="The text of the targeting")
    targeting_type: str | None = Field(..., description="The type of targeting")
    top_of_search_impression_share: float | None = Field(..., description="The impression share at the top of search")
