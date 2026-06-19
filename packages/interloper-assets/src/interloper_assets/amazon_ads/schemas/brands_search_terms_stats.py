import datetime

from interloper.schema import Schema
from pydantic import Field


class BrandsSearchTermsStats(Schema):
    """Brands search term performance metrics including clicks, impressions, purchases, and keyword details."""

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    campaign_budget_amount: float | None = Field(..., description="The amount of the campaign budget")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_budget_type: str | None = Field(..., description="The type of campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_status: str | None = Field(..., description="The status of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    cost: float | None = Field(..., description="The cost of the ad")
    cost_type: str | None = Field(..., description="The type of cost")
    date: datetime.date | None = Field(..., description="The date of the search term data")
    impressions: float | None = Field(..., description="The number of impressions")
    keyword_bid: float | None = Field(..., description="The bid amount for the keyword")
    keyword_id: int | None = Field(..., description="The ID of the keyword")
    keyword_text: str | None = Field(..., description="The text of the keyword")
    match_type: str | None = Field(..., description="The type of match for the keyword")
    purchases: float | None = Field(..., description="The number of purchases made")
    purchases_clicks: float | None = Field(..., description="The number of purchases made from clicks")
    sales: float | None = Field(..., description="The total sales amount")
    sales_clicks: float | None = Field(..., description="The number of sales made from clicks")
    search_term: str | None = Field(..., description="The search term used")
    units_sold: float | None = Field(..., description="The number of units sold")
    video_5_second_view_rate: float | None = Field(
        ..., description="The rate at which viewers watched at least 5 seconds of the video"
    )
    video_5_second_views: float | None = Field(
        ..., description="The number of views for at least 5 seconds of the video"
    )
    video_complete_views: float | None = Field(..., description="The number of complete video views")
    video_first_quartile_views: float | None = Field(
        ..., description="The number of views for at least 25% of the video"
    )
    video_midpoint_views: float | None = Field(..., description="The number of views for at least 50% of the video")
    video_third_quartile_views: float | None = Field(
        ..., description="The number of views for at least 75% of the video"
    )
    video_unmutes: float | None = Field(..., description="The number of video unmutes")
    view_click_through_rate: float | None = Field(..., description="The rate at which viewers clicked through the ad")
    viewability_rate: float | None = Field(..., description="The rate at which the ad was viewable")
    viewable_impressions: float | None = Field(..., description="The number of viewable impressions")
