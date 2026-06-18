import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdGroupsStats(Schema):
    """The Trade Desk ad group performance metrics."""

    ad_group_id: str | None = Field(default=None, description="Ad group ID")
    ad_group_name: str | None = Field(default=None, description="Ad group name")
    ad_group_budget: float | None = Field(default=None, description="Ad group budget")
    ad_group_daily_budget: float | None = Field(default=None, description="Ad group daily budget")
    campaign_id: str | None = Field(default=None, description="Campaign ID")
    campaign_name: str | None = Field(default=None, description="Campaign name")
    advertiser_id: str | None = Field(default=None, description="Advertiser ID")
    advertiser_name: str | None = Field(default=None, description="Advertiser name")
    advertiser_currency: str | None = Field(default=None, description="Advertiser currency code")
    partner_id: str | None = Field(default=None, description="Partner ID")
    partner_name: str | None = Field(default=None, description="Partner name")
    impressions: int | None = Field(default=None, description="Number of impressions")
    clicks: int | None = Field(default=None, description="Number of clicks")
    ctr: float | None = Field(default=None, description="Click-through rate")
    total_cost_advertiser_currency: float | None = Field(
        default=None, description="Total cost in advertiser currency"
    )
    cpm_advertiser_currency: float | None = Field(default=None, description="CPM in advertiser currency")
    cpc_advertiser_currency: float | None = Field(default=None, description="CPC in advertiser currency")
    total_conversions: int | None = Field(default=None, description="Total number of conversions")
    click_through_conversions: int | None = Field(default=None, description="Click-through conversions")
    view_through_conversions: int | None = Field(default=None, description="View-through conversions")
    total_conversion_value_in_advertiser_currency: float | None = Field(
        default=None, description="Total conversion value in advertiser currency"
    )
    video_completions: int | None = Field(default=None, description="Video completions")
    video_completion_rate: float | None = Field(default=None, description="Video completion rate")
    video_25_percent_complete: int | None = Field(default=None, description="Video 25% complete views")
    video_50_percent_complete: int | None = Field(default=None, description="Video 50% complete views")
    video_75_percent_complete: int | None = Field(default=None, description="Video 75% complete views")
    date: dt.date | None = Field(default=None, description="Report date")
