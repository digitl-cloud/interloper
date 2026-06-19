import datetime

from interloper.schema import Schema
from pydantic import Field


class DisplayAdvertisedProductsStats(Schema):
    """Display advertised product performance metrics including clicks, impressions, purchases, and viewability."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    ad_id: int | None = Field(..., description="The ID of the ad")
    add_to_cart: float | None = Field(..., description="The number of add to cart")
    add_to_cart_clicks: float | None = Field(..., description="The number of add to cart clicks")
    add_to_cart_rate: float | None = Field(..., description="The rate of add to cart")
    add_to_cart_views: float | None = Field(..., description="The number of add to cart views")
    bid_optimization: str | None = Field(..., description="The bid optimization")
    branded_search_rate: float | None = Field(..., description="The rate of branded searches")
    branded_searches: float | None = Field(..., description="The number of branded searches")
    branded_searches_clicks: float | None = Field(..., description="The number of branded searches clicks")
    branded_searches_views: float | None = Field(..., description="The number of branded searches views")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost")
    cumulative_reach: float | None = Field(..., description="The cumulative reach")
    date: datetime.date | None = Field(..., description="The date of the advertisement")
    detail_page_views: float | None = Field(..., description="The number of detail page views")
    detail_page_views_clicks: float | None = Field(..., description="The number of detail page views clicks")
    ecp_add_to_cart: float | None = Field(..., description="The ECP of add to cart")
    ecp_brand_search: float | None = Field(..., description="The ECP of brand search")
    impressions: float | None = Field(..., description="The number of impressions")
    impressions_frequency_average: float | None = Field(..., description="The average frequency of impressions")
    impressions_views: float | None = Field(..., description="The number of impressions views")
    new_to_brand_detail_page_view_clicks: float | None = Field(
        ..., description="The number of new-to-brand detail page view clicks"
    )
    new_to_brand_detail_page_view_rate: float | None = Field(
        ..., description="The rate of new-to-brand detail page views"
    )
    new_to_brand_detail_page_view_views: float | None = Field(
        ..., description="The number of new-to-brand detail page view views"
    )
    new_to_brand_detail_page_views: float | None = Field(
        ..., description="The number of new-to-brand detail page views"
    )
    new_to_brand_ecp_detail_page_view: float | None = Field(
        ..., description="The ECP of new-to-brand detail page views"
    )
    new_to_brand_purchases: float | None = Field(..., description="The number of new-to-brand purchases")
    new_to_brand_purchases_clicks: float | None = Field(..., description="The number of new-to-brand purchases clicks")
    new_to_brand_sales: float | None = Field(..., description="The number of new-to-brand sales")
    new_to_brand_sales_clicks: float | None = Field(..., description="The number of new-to-brand sales clicks")
    new_to_brand_units_sold: float | None = Field(..., description="The number of new-to-brand units sold")
    new_to_brand_units_sold_clicks: float | None = Field(
        ..., description="The number of new-to-brand units sold clicks"
    )
    promoted_asin: str | None = Field(..., description="The ASIN of the promoted product")
    promoted_sku: str | None = Field(..., description="The SKU of the promoted product")
    purchases: float | None = Field(..., description="The number of purchases")
    purchases_clicks: float | None = Field(..., description="The number of purchases clicks")
    purchases_promoted_clicks: float | None = Field(..., description="The number of purchases promoted clicks")
    sales: float | None = Field(..., description="The number of sales")
    sales_clicks: float | None = Field(..., description="The number of sales clicks")
    sales_promoted_clicks: float | None = Field(..., description="The number of sales promoted clicks")
    units_sold: float | None = Field(..., description="The number of units sold")
    units_sold_clicks: float | None = Field(..., description="The number of units sold clicks")
    video_complete_views: float | None = Field(..., description="The number of video complete views")
    video_first_quartile_views: float | None = Field(..., description="The number of video first quartile views")
    video_midpoint_views: float | None = Field(..., description="The number of video midpoint views")
    video_third_quartile_views: float | None = Field(..., description="The number of video third quartile views")
    video_unmutes: float | None = Field(..., description="The number of video unmutes")
    view_click_through_rate: float | None = Field(..., description="The click-through rate of views")
    viewability_rate: float | None = Field(..., description="The rate of viewability")
