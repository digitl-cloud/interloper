import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class TelevisionTargetingStats(Schema):
    """Sponsored Television performance by targeting. One row per targeting and day."""

    add_to_cart: int | None = Field(default=None, description="Attributed add to cart.")
    add_to_cart_clicks: int | None = Field(default=None, description="Attributed add to cart clicks.")
    add_to_cart_views: int | None = Field(default=None, description="Attributed add to cart views.")
    branded_searches: int | None = Field(default=None, description="Attributed branded searches.")
    branded_searches_clicks: int | None = Field(default=None, description="Attributed branded searches clicks.")
    branded_searches_views: int | None = Field(default=None, description="Attributed branded searches views.")
    clicks: int | None = Field(default=None, description="Clicks.")
    click_through_rate: float | None = Field(default=None, description="Clicks per impression.")
    cost: float | None = Field(default=None, description="Amount spent.")
    cost_per_thousand_impressions: float | None = Field(default=None, description="Cost per thousand impressions.")
    date: dt.date | None = Field(default=None, description="The report day.")
    detail_page_views: int | None = Field(default=None, description="Attributed detail page views.")
    detail_page_views_clicks: int | None = Field(default=None, description="Attributed detail page views clicks.")
    detail_page_views_views: int | None = Field(default=None, description="Attributed detail page views views.")
    impressions: int | None = Field(default=None, description="Impressions.")
    keyword: str | None = Field(default=None, description="Targeted keyword.")
    keyword_bid: float | None = Field(default=None, description="Keyword bid.")
    keyword_id: str | None = Field(default=None, description="Keyword id.")
    keyword_type: str | None = Field(default=None, description="Keyword type.")
    match_type: str | None = Field(default=None, description="Keyword match type.")
    new_to_brand_detail_page_view_clicks: int | None = Field(
        default=None, description="Attributed new to brand detail page view clicks."
    )
    new_to_brand_detail_page_views: int | None = Field(
        default=None, description="Attributed new to brand detail page views."
    )
    new_to_brand_detail_page_view_views: int | None = Field(
        default=None, description="Attributed new to brand detail page view views."
    )
    new_to_brand_purchases: int | None = Field(default=None, description="Attributed new to brand purchases.")
    new_to_brand_purchases_clicks: int | None = Field(
        default=None, description="Attributed new to brand purchases clicks."
    )
    new_to_brand_purchases_views: int | None = Field(
        default=None, description="Attributed new to brand purchases views."
    )
    new_to_brand_sales: float | None = Field(default=None, description="Attributed new to brand sales.")
    new_to_brand_sales_clicks: float | None = Field(default=None, description="Attributed new to brand sales clicks.")
    new_to_brand_sales_views: float | None = Field(default=None, description="Attributed new to brand sales views.")
    portfolio_id: str | None = Field(default=None, description="Portfolio id.")
    purchases: int | None = Field(default=None, description="Attributed purchases.")
    purchases_clicks: int | None = Field(default=None, description="Attributed purchases clicks.")
    purchases_views: int | None = Field(default=None, description="Attributed purchases views.")
    roas: float | None = Field(default=None, description="Sales divided by cost.")
    sales: float | None = Field(default=None, description="Attributed sales.")
    sales_clicks: float | None = Field(default=None, description="Attributed sales clicks.")
    sales_views: float | None = Field(default=None, description="Attributed sales views.")
    targeting_text: str | None = Field(default=None, description="Targeting expression.")
    units_sold: int | None = Field(default=None, description="Attributed units sold.")
    units_sold_clicks: int | None = Field(default=None, description="Attributed units sold clicks.")
    units_sold_views: int | None = Field(default=None, description="Attributed units sold views.")
    video_complete_views: int | None = Field(default=None, description="Video complete views.")
    video_first_quartile_views: int | None = Field(default=None, description="Video first quartile views.")
    video_midpoint_views: int | None = Field(default=None, description="Video midpoint views.")
    video_third_quartile_views: int | None = Field(default=None, description="Video third quartile views.")
