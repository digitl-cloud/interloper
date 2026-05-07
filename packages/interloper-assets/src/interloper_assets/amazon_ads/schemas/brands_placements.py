import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsPlacements(Schema):
    """Brands placement performance metrics including clicks, impressions, and engagement by ad placement."""

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
    date: datetime.date | None = Field(..., description="The date of the data entry")
    detail_page_views: float | None = Field(..., description="The number of detail page views")
    detail_page_views_clicks: float | None = Field(..., description="The number of clicks for detail page views")
    ecp_add_to_cart: float | None = Field(..., description="The eCP add to cart")
    impressions: float | None = Field(..., description="The number of impressions")
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
        ..., description="The ECP detail page view for new-to-brand"
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
    sales: float | None = Field(..., description="The total sales")
    sales_clicks: float | None = Field(..., description="The number of clicks for sales")
    sales_promoted: float | None = Field(..., description="The number of promoted sales")
    units_sold: float | None = Field(..., description="The number of units sold")
    units_sold_clicks: float | None = Field(..., description="The number of clicks for units sold")
    video_5_second_view_rate: float | None = Field(..., description="The rate of video 5-second views")
    video_5_second_views: float | None = Field(..., description="The number of video 5-second views")
    video_complete_views: float | None = Field(..., description="The number of complete video views")
    video_first_quartile_views: float | None = Field(..., description="The number of video first quartile views")
    video_midpoint_views: float | None = Field(..., description="The number of video midpoint views")
    video_third_quartile_views: float | None = Field(..., description="The number of video third quartile views")
    video_unmutes: float | None = Field(..., description="The number of video unmutes")
    view_click_through_rate: float | None = Field(..., description="The rate of view click-through")
    viewability_rate: float | None = Field(..., description="The rate of viewability")
    viewable_impressions: float | None = Field(..., description="The number of viewable impressions")
