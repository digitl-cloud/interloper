import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsAdsStats(Schema):
    """Brands individual ad performance metrics including impressions, clicks, conversions, and new-to-brand metrics."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    ad_id: int | None = Field(..., description="The ID of the ad")
    add_to_cart: float | None = Field(..., description="The number of items added to cart")
    add_to_cart_clicks: float | None = Field(..., description="The number of clicks resulting in adding to cart")
    add_to_cart_rate: float | None = Field(..., description="The rate of adding to cart")
    branded_searches: float | None = Field(..., description="The number of searches for the brand")
    branded_searches_clicks: float | None = Field(
        ..., description="The number of clicks resulting from branded searches"
    )
    campaign_budget_amount: float | None = Field(..., description="The budget amount for the ad campaign")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code for the ad campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of budget for the ad campaign")
    campaign_id: int | None = Field(..., description="The ID of the ad campaign")
    campaign_name: str | None = Field(..., description="The name of the ad campaign")
    campaign_status: str | None = Field(..., description="The status of the ad campaign")
    clicks: float | None = Field(..., description="The number of ad clicks")
    cost: float | None = Field(..., description="The total cost")
    cost_type: str | None = Field(..., description="The type of cost associated with the ad campaign")
    date: datetime.date | None = Field(..., description="The date of the ad performance data")
    detail_page_views: float | None = Field(..., description="The number of views of the ad's detail page")
    detail_page_views_clicks: float | None = Field(
        ..., description="The number of clicks resulting in detail page views"
    )
    ecp_add_to_cart: float | None = Field(..., description="The effective cost per add to cart")
    impressions: float | None = Field(..., description="The number of ad impressions")
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
        ..., description="The effective cost per detail page view from new-to-brand customers"
    )
    new_to_brand_purchases: float | None = Field(..., description="The number of purchases from new-to-brand customers")
    new_to_brand_purchases_clicks: float | None = Field(
        ..., description="The number of clicks resulting in purchases from new-to-brand customers"
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
    sales_clicks: float | None = Field(..., description="The number of clicks resulting in sales")
    sales_promoted: float | None = Field(..., description="The total amount of promoted sales")
    units_sold: float | None = Field(..., description="The number of units sold")
    units_sold_clicks: float | None = Field(..., description="The number of clicks resulting in units sold")
    video_5_second_view_rate: float | None = Field(..., description="The rate of 5-second views of video ads")
    video_5_second_views: float | None = Field(..., description="The number of 5-second views of video ads")
    video_complete_views: float | None = Field(..., description="The number of complete views of video ads")
    video_first_quartile_views: float | None = Field(..., description="The number of first quartile views of video ads")
    video_midpoint_views: float | None = Field(..., description="The number of midpoint views of video ads")
    video_third_quartile_views: float | None = Field(..., description="The number of third quartile views of video ads")
    video_unmutes: float | None = Field(..., description="The number of unmutes of video ads")
    viewability_rate: float | None = Field(..., description="The rate of ad viewability")
    viewable_impressions: float | None = Field(..., description="The number of viewable ad impressions")
