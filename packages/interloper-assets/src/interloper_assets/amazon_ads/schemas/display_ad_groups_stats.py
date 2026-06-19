import datetime

from interloper.schema import Schema
from pydantic import Field


class DisplayAdGroupsStats(Schema):
    """Display ad group performance metrics including clicks, impressions, purchases, sales, and viewability rates."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group.")
    ad_group_name: str | None = Field(..., description="The name of the ad group.")
    add_to_cart: float | None = Field(..., description="The number of add to cart.")
    add_to_cart_clicks: float | None = Field(..., description="The number of add to cart clicks.")
    add_to_cart_rate: float | None = Field(..., description="The rate of add to cart.")
    add_to_cart_views: float | None = Field(..., description="The number of add to cart views.")
    bid_optimization: str | None = Field(..., description="The bid optimization strategy.")
    branded_search_rate: float | None = Field(..., description="The rate of branded searches.")
    branded_searches: float | None = Field(..., description="The number of branded searches.")
    branded_searches_clicks: float | None = Field(..., description="The number of branded searches clicks.")
    branded_searches_views: float | None = Field(..., description="The number of branded searches views.")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget.")
    campaign_id: int | None = Field(..., description="The ID of the campaign.")
    campaign_name: str | None = Field(..., description="The name of the campaign.")
    clicks: float | None = Field(..., description="The number of clicks.")
    cost: float | None = Field(..., description="The cost of the ad group.")
    date: datetime.date | None = Field(..., description="The date of the ad group.")
    detail_page_views: float | None = Field(..., description="The number of detail page views.")
    detail_page_views_clicks: float | None = Field(..., description="The number of detail page views clicks.")
    ecp_add_to_cart: float | None = Field(..., description="The effective cost per add to cart.")
    ecp_brand_search: float | None = Field(..., description="The effective cost per brand search.")
    impressions: float | None = Field(..., description="The number of impressions.")
    impressions_views: float | None = Field(..., description="The number of impressions views.")
    new_to_brand_purchases: float | None = Field(..., description="The number of new-to-brand purchases.")
    new_to_brand_purchases_clicks: float | None = Field(..., description="The number of new-to-brand purchases clicks.")
    new_to_brand_sales: float | None = Field(..., description="The number of new-to-brand sales.")
    new_to_brand_sales_clicks: float | None = Field(..., description="The number of new-to-brand sales clicks.")
    new_to_brand_units_sold: float | None = Field(..., description="The number of new-to-brand units sold.")
    new_to_brand_units_sold_clicks: float | None = Field(
        ..., description="The number of new-to-brand units sold clicks."
    )
    purchases: float | None = Field(..., description="The number of purchases.")
    purchases_clicks: float | None = Field(..., description="The number of purchases clicks.")
    purchases_promoted_clicks: float | None = Field(..., description="The number of purchases promoted clicks.")
    sales: float | None = Field(..., description="The number of sales.")
    sales_clicks: float | None = Field(..., description="The number of sales clicks.")
    sales_promoted_clicks: float | None = Field(..., description="The number of sales promoted clicks.")
    units_sold: float | None = Field(..., description="The number of units sold.")
    units_sold_clicks: float | None = Field(..., description="The number of units sold clicks.")
    video_complete_views: float | None = Field(..., description="The number of video complete views.")
    video_first_quartile_views: float | None = Field(..., description="The number of video first quartile views.")
    video_midpoint_views: float | None = Field(..., description="The number of video midpoint views.")
    video_third_quartile_views: float | None = Field(..., description="The number of video third quartile views.")
    video_unmutes: float | None = Field(..., description="The number of video unmutes.")
    view_click_through_rate: float | None = Field(..., description="The rate of view click through.")
    viewability_rate: float | None = Field(..., description="The rate of viewability.")
