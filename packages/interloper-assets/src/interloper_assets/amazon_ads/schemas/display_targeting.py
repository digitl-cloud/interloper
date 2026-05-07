import datetime

from interloper.schema import Schema
from pydantic import Field


class DisplayTargeting(Schema):
    """Display targeting performance metrics including clicks, impressions, and targeting expression details."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    ad_keyword_status: str | None = Field(..., description="The status of the ad keyword")
    add_to_cart: float | None = Field(..., description="The number of add to cart")
    add_to_cart_clicks: float | None = Field(..., description="The number of clicks for add to cart")
    add_to_cart_rate: float | None = Field(..., description="The rate of add to cart")
    add_to_cart_views: float | None = Field(..., description="The number of add to cart views")
    branded_search_rate: float | None = Field(..., description="The rate of branded searches")
    branded_searches: float | None = Field(..., description="The number of branded searches")
    branded_searches_clicks: float | None = Field(..., description="The number of clicks for branded searches")
    branded_searches_views: float | None = Field(..., description="The number of views for branded searches")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost")
    date: datetime.date | None = Field(..., description="The date of the data entry")
    detail_page_views: float | None = Field(..., description="The number of detail page views")
    detail_page_views_clicks: float | None = Field(..., description="The number of clicks for detail page views")
    ecp_add_to_cart: float | None = Field(..., description="The ECP for add to cart")
    ecp_brand_search: float | None = Field(..., description="The ECP for brand search")
    impressions: float | None = Field(..., description="The number of impressions")
    impressions_views: float | None = Field(..., description="The number of views for impressions")
    new_to_brand_detail_page_view_clicks: float | None = Field(
        ..., description="The number of clicks for new-to-brand detail page views"
    )
    new_to_brand_detail_page_view_rate: float | None = Field(
        ..., description="The rate of new-to-brand detail page views"
    )
    new_to_brand_detail_page_view_views: float | None = Field(
        ..., description="The number of views for new-to-brand detail page views"
    )
    new_to_brand_detail_page_views: float | None = Field(
        ..., description="The number of detail page views for new-to-brand"
    )
    new_to_brand_ecp_detail_page_view: float | None = Field(
        ..., description="The ECP for new-to-brand detail page views"
    )
    new_to_brand_purchases: float | None = Field(..., description="The number of new-to-brand purchases")
    new_to_brand_purchases_clicks: float | None = Field(
        ..., description="The number of clicks for new-to-brand purchases"
    )
    new_to_brand_sales: float | None = Field(..., description="The number of new-to-brand sales")
    new_to_brand_sales_clicks: float | None = Field(..., description="The number of clicks for new-to-brand sales")
    new_to_brand_units_sold: float | None = Field(..., description="The number of units sold for new-to-brand")
    new_to_brand_units_sold_clicks: float | None = Field(
        ..., description="The number of clicks for units sold for new-to-brand"
    )
    purchases: float | None = Field(..., description="The number of purchases")
    purchases_clicks: float | None = Field(..., description="The number of clicks for purchases")
    purchases_promoted_clicks: float | None = Field(..., description="The number of clicks for purchases promoted")
    sales: float | None = Field(..., description="The number of sales")
    sales_clicks: float | None = Field(..., description="The number of clicks for sales")
    sales_promoted_clicks: float | None = Field(..., description="The number of clicks for sales promoted")
    targeting_expression: str | None = Field(..., description="The expression of the targeting")
    targeting_id: int | None = Field(..., description="The ID of the targeting")
    targeting_text: str | None = Field(..., description="The text of the targeting")
    units_sold: float | None = Field(..., description="The number of units sold")
    units_sold_clicks: float | None = Field(..., description="The number of clicks for units sold")
    video_complete_views: float | None = Field(..., description="The number of complete video views")
    video_first_quartile_views: float | None = Field(..., description="The number of views for video first quartile")
    video_midpoint_views: float | None = Field(..., description="The number of views for video midpoint")
    video_third_quartile_views: float | None = Field(..., description="The number of views for video third quartile")
    video_unmutes: float | None = Field(..., description="The number of video unmutes")
    view_click_through_rate: float | None = Field(..., description="The click-through rate for views")
    viewability_rate: float | None = Field(..., description="The rate of viewability")
