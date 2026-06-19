import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsAdGroupsStats(Schema):
    """Brands ad group performance metrics including impressions, clicks, conversions, and new-to-brand metrics."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    ad_status: str | None = Field(..., description="The status of the ad")
    add_to_cart: float | None = Field(..., description="The number of items added to cart")
    add_to_cart_clicks: float | None = Field(..., description="The number of clicks resulting in adding to cart")
    add_to_cart_rate: float | None = Field(..., description="The rate of adding to cart")
    branded_searches: float | None = Field(..., description="The number of branded searches")
    branded_searches_clicks: float | None = Field(
        ..., description="The number of clicks resulting from branded searches"
    )
    campaign_budget_amount: float | None = Field(..., description="The budget amount for the campaign")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code for the campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of budget for the campaign")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_status: str | None = Field(..., description="The status of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost of the ad")
    cost_type: str | None = Field(..., description="The type of cost")
    date: datetime.date | None = Field(..., description="The date of the data entry")
    detail_page_views: float | None = Field(..., description="The number of views of the detail page")
    detail_page_views_clicks: float | None = Field(
        ..., description="The number of clicks resulting in detail page views"
    )
    ecp_add_to_cart: float | None = Field(..., description="The ECP (effective cost per) add to cart")
    impressions: float | None = Field(..., description="The number of impressions")
    new_to_brand_detail_page_view_rate: float | None = Field(
        ..., description="The rate of detail page views from new-to-brand customers"
    )
    new_to_brand_detail_page_views: float | None = Field(
        ..., description="The number of detail page views from new-to-brand customers"
    )
    new_to_brand_detail_page_views_clicks: float | None = Field(
        ..., description="The number of clicks resulting in detail page views from new-to-brand customers"
    )
    new_to_brand_ecp_detail_page_view: float | None = Field(
        ..., description="The ECP (effective cost per) detail page view from new-to-brand customers"
    )
    new_to_brand_purchases: float | None = Field(..., description="The number of new-to-brand purchases")
    new_to_brand_purchases_clicks: float | None = Field(
        ..., description="The number of clicks resulting in new-to-brand purchases"
    )
    new_to_brand_purchases_percentage: float | None = Field(
        ..., description="The percentage of purchases from new-to-brand customers"
    )
    new_to_brand_purchases_rate: float | None = Field(
        ..., description="The rate of purchases from new-to-brand customers"
    )
    new_to_brand_sales: float | None = Field(..., description="The total sales amount from new-to-brand customers")
    new_to_brand_sales_clicks: float | None = Field(
        ..., description="The number of sales clicks from new-to-brand customers"
    )
    new_to_brand_sales_percentage: float | None = Field(
        ..., description="The percentage of sales from new-to-brand customers"
    )
    new_to_brand_units_sold: float | None = Field(
        ..., description="The number of units sold from new-to-brand customers"
    )
    new_to_brand_units_sold_clicks: float | None = Field(
        ..., description="The number of clicks resulting in units sold from new-to-brand customers"
    )
    new_to_brand_units_sold_percentage: float | None = Field(
        ..., description="The percentage of units sold from new-to-brand customers"
    )
    purchases: float | None = Field(..., description="The number of purchases")
    purchases_clicks: float | None = Field(..., description="The number of clicks resulting in purchases")
    purchases_promoted: float | None = Field(..., description="The number of promoted purchases")
    sales: float | None = Field(..., description="The total sales amount")
    sales_clicks: float | None = Field(..., description="The number of sales clicks")
    sales_promoted: float | None = Field(..., description="The total promoted sales amount")
    units_sold: float | None = Field(..., description="The number of units sold")
    units_sold_clicks: float | None = Field(..., description="The number of clicks resulting in units sold")
    video_5_second_view_rate: float | None = Field(..., description="The rate of video 5-second views")
    video_5_second_views: float | None = Field(..., description="The number of video 5-second views")
    video_complete_views: float | None = Field(..., description="The number of complete video views")
    video_first_quartile_views: float | None = Field(..., description="The number of video first quartile views")
    video_midpoint_views: float | None = Field(..., description="The number of video midpoint views")
    video_third_quartile_views: float | None = Field(..., description="The number of video third quartile views")
    video_unmutes: float | None = Field(..., description="The number of video unmutes")
    viewability_rate: float | None = Field(..., description="The rate of viewability for the ad")
