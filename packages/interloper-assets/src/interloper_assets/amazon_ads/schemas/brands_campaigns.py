import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsCampaigns(Schema):
    """Brands campaign performance metrics including impressions, clicks, conversions, and new-to-brand metrics."""

    add_to_cart: float | None = Field(..., description="The number of add to cart")
    add_to_cart_clicks: float | None = Field(..., description="The number of add to cart clicks")
    add_to_cart_rate: float | None = Field(..., description="The rate of add to cart")
    branded_searches: float | None = Field(..., description="The number of branded searches")
    branded_searches_clicks: float | None = Field(..., description="The number of branded searches clicks")
    campaign_budget_amount: float | None = Field(..., description="The budget amount for the campaign")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code for the campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_rule_based_budget_amount: float | None = Field(
        ..., description="The budget amount based on campaign rules"
    )
    campaign_status: str | None = Field(..., description="The status of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost")
    cost_type: str | None = Field(..., description="The type of cost")
    date: datetime.date | None = Field(..., description="The date of the campaign")
    detail_page_views: float | None = Field(..., description="The number of detail page views")
    detail_page_views_clicks: float | None = Field(..., description="The number of detail page views clicks")
    ecp_add_to_cart: float | None = Field(..., description="The eCP (Effective Cost per Add to Cart)")
    impressions: float | None = Field(..., description="The number of impressions")
    kindle_edition_normalized_pages_read_14d: float | None = Field(
        ..., description="The number of Kindle Edition Normalized Pages read in the last 14 days"
    )
    kindle_edition_normalized_pages_royalties_14d: float | None = Field(
        ..., description="The royalties for Kindle Edition Normalized Pages read in the last 14 days"
    )
    new_to_brand_detail_page_view_rate: float | None = Field(
        ..., description="The rate of new-to-brand detail page views"
    )
    new_to_brand_detail_page_views: float | None = Field(
        ..., description="The number of new-to-brand detail page views"
    )
    new_to_brand_detail_page_views_clicks: float | None = Field(
        ..., description="The number of new-to-brand detail page views clicks"
    )
    new_to_brand_ecp_detail_page_view: float | None = Field(
        ..., description="The ECP (Effective Cost per Detail Page View) for new-to-brand"
    )
    new_to_brand_purchases: float | None = Field(..., description="The number of new-to-brand purchases")
    new_to_brand_purchases_clicks: float | None = Field(..., description="The number of new-to-brand purchases clicks")
    new_to_brand_purchases_percentage: float | None = Field(..., description="The percentage of new-to-brand purchases")
    new_to_brand_purchases_rate: float | None = Field(..., description="The rate of new-to-brand purchases")
    new_to_brand_sales: float | None = Field(..., description="The number of new-to-brand sales")
    new_to_brand_sales_clicks: float | None = Field(..., description="The number of new-to-brand sales clicks")
    new_to_brand_sales_percentage: float | None = Field(..., description="The percentage of new-to-brand sales")
    new_to_brand_units_sold: float | None = Field(..., description="The number of new-to-brand units sold")
    new_to_brand_units_sold_clicks: float | None = Field(
        ..., description="The number of new-to-brand units sold clicks"
    )
    new_to_brand_units_sold_percentage: float | None = Field(
        ..., description="The percentage of new-to-brand units sold"
    )
    purchases: float | None = Field(..., description="The number of purchases")
    purchases_clicks: float | None = Field(..., description="The number of purchases clicks")
    purchases_promoted: float | None = Field(..., description="The number of promoted purchases")
    sales: float | None = Field(..., description="The total sales")
    sales_clicks: float | None = Field(..., description="The number of sales clicks")
    sales_promoted: float | None = Field(..., description="The number of promoted sales")
    top_of_search_impression_share: float | None = Field(..., description="The impression share at the top of search")
    units_sold: float | None = Field(..., description="The number of units sold")
    units_sold_clicks: float | None = Field(..., description="The number of units sold clicks")
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
