import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStats(Schema):
    """Campaign performance metrics per day (CM360 STANDARD report at campaign/activity grain)."""

    active_view_average_viewable_time_seconds: float | None = Field(
        default=None, description="The average viewable time in seconds"
    )
    active_view_measurable_impressions: int | None = Field(
        default=None, description="The number of active view measurable impressions"
    )
    active_view_pct_viewable_impressions: float | None = Field(
        default=None, description="The percentage of active view viewable impressions"
    )
    active_view_pct_visible_at_completion: float | None = Field(
        default=None, description="The percentage of visible impressions at completion"
    )
    active_view_viewable_impressions: int | None = Field(
        default=None, description="The number of active view viewable impressions"
    )
    activity: str | None = Field(default=None, description="The name of the activity")
    activity_id: str | None = Field(default=None, description="The ID of the activity")
    advertiser: str | None = Field(default=None, description="The name of the advertiser")
    advertiser_id: str | None = Field(default=None, description="The ID of the advertiser")
    booked_activities: float | None = Field(default=None, description="The number of booked activities")
    booked_clicks: float | None = Field(default=None, description="The number of booked clicks")
    booked_impressions: float | None = Field(default=None, description="The number of booked impressions")
    booked_viewable_impressions: float | None = Field(
        default=None, description="The number of booked viewable impressions"
    )
    campaign: str | None = Field(default=None, description="The name of the campaign")
    campaign_end_date: dt.date | None = Field(default=None, description="The end date of the campaign")
    campaign_id: str | None = Field(default=None, description="The ID of the campaign")
    campaign_start_date: dt.date | None = Field(default=None, description="The start date of the campaign")
    click_rate: float | None = Field(default=None, description="The click rate")
    click_through_conversions: float | None = Field(default=None, description="The number of click-through conversions")
    click_through_revenue: float | None = Field(default=None, description="The click-through revenue")
    clicks: int | None = Field(default=None, description="The number of clicks")
    companion_clicks: int | None = Field(default=None, description="The number of companion clicks")
    companion_views: int | None = Field(default=None, description="The number of companion views")
    date: dt.date | None = Field(default=None, description="The date of the campaign")
    dv_360_cost_account_currency: float | None = Field(default=None, description="The DV 360 cost in account currency")
    dv_360_cost_usd: float | None = Field(default=None, description="The DV 360 cost in USD")
    dynamic_profile: str | None = Field(default=None, description="The dynamic profile")
    dynamic_profile_id: str | None = Field(default=None, description="The ID of the dynamic profile")
    effective_cpm: float | None = Field(default=None, description="The effective CPM")
    impressions: int | None = Field(default=None, description="The number of impressions")
    media_cost: float | None = Field(default=None, description="The media cost")
    planned_media_cost: float | None = Field(default=None, description="The planned media cost")
    platform_type: str | None = Field(default=None, description="The platform type")
    total_conversions: float | None = Field(default=None, description="The total number of conversions")
    total_revenue: float | None = Field(default=None, description="The total revenue")
    true_view_views: int | None = Field(default=None, description="The number of TrueView views")
    video_average_view_time: float | None = Field(default=None, description="The average view time for videos")
    video_companion_clicks: int | None = Field(default=None, description="The number of video companion clicks")
    video_completions: int | None = Field(default=None, description="The number of video completions")
    video_first_quartile_completions: int | None = Field(
        default=None, description="The number of video first quartile completions"
    )
    video_midpoints: int | None = Field(default=None, description="The number of video midpoints")
    video_plays: int | None = Field(default=None, description="The number of video plays")
    video_third_quartile_completions: int | None = Field(
        default=None, description="The number of video third quartile completions"
    )
    video_views: int | None = Field(default=None, description="The number of video views")
    view_through_conversions: float | None = Field(default=None, description="The number of view-through conversions")
    view_through_revenue: float | None = Field(default=None, description="The view-through revenue")
