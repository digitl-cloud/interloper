import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsStats(Schema):
    """Ad performance metrics per day (CM360 STANDARD report at ad/placement/creative grain)."""

    active_view_average_viewable_time_seconds: float | None = Field(
        default=None, description="Active View Average Viewable Time (Seconds)"
    )
    active_view_measurable_impressions: int | None = Field(
        default=None, description="Active View Measurable Impressions"
    )
    active_view_pct_viewable_impressions: float | None = Field(
        default=None, description="Active View Percentage of Viewable Impressions"
    )
    active_view_pct_visible_at_completion: float | None = Field(
        default=None, description="Active View Percentage Visible at Completion"
    )
    active_view_viewable_impressions: int | None = Field(default=None, description="Active View Viewable Impressions")
    activity: str | None = Field(default=None, description="Activity")
    activity_id: str | None = Field(default=None, description="Activity ID")
    ad: str | None = Field(default=None, description="Ad")
    ad_id: str | None = Field(default=None, description="Ad ID")
    ad_status: str | None = Field(default=None, description="Ad Status")
    advertiser: str | None = Field(default=None, description="Name of the advertiser")
    advertiser_id: str | None = Field(default=None, description="ID of the advertiser")
    booked_activities: float | None = Field(default=None, description="Booked Activities")
    booked_clicks: float | None = Field(default=None, description="Booked Clicks")
    booked_impressions: float | None = Field(default=None, description="Booked Impressions")
    booked_viewable_impressions: float | None = Field(default=None, description="Booked Viewable Impressions")
    campaign: str | None = Field(default=None, description="Name of the campaign")
    campaign_end_date: dt.date | None = Field(default=None, description="End date of the campaign")
    campaign_id: str | None = Field(default=None, description="ID of the campaign")
    campaign_start_date: dt.date | None = Field(default=None, description="Start date of the campaign")
    click_rate: float | None = Field(default=None, description="Click Rate")
    click_through_conversions: float | None = Field(default=None, description="Click-Through Conversions")
    click_through_revenue: float | None = Field(default=None, description="Click-Through Revenue")
    clicks: int | None = Field(default=None, description="Clicks")
    companion_clicks: int | None = Field(default=None, description="Companion Clicks")
    companion_views: int | None = Field(default=None, description="Companion Views")
    creative: str | None = Field(default=None, description="Creative")
    creative_id: str | None = Field(default=None, description="Creative ID")
    creative_pixel_size: str | None = Field(default=None, description="Pixel size of the placement")
    creative_type: str | None = Field(default=None, description="Creative Type")
    date: dt.date | None = Field(default=None, description="Date of the ad")
    dv_360_cost_account_currency: float | None = Field(default=None, description="DV 360 Cost (Account Currency)")
    dv_360_cost_usd: float | None = Field(default=None, description="DV 360 Cost (USD)")
    dynamic_profile: str | None = Field(default=None, description="Dynamic Profile")
    dynamic_profile_id: str | None = Field(default=None, description="Dynamic Profile ID")
    effective_cpm: float | None = Field(default=None, description="Effective CPM")
    impressions: int | None = Field(default=None, description="Impressions")
    media_cost: float | None = Field(default=None, description="Media Cost")
    package_roadblock: str | None = Field(default=None, description="Name of the package roadblock")
    package_roadblock_id: str | None = Field(default=None, description="ID of the package roadblock")
    package_roadblock_total_booked_units: str | None = Field(
        default=None, description="Package Roadblock Total Booked Units"
    )
    placement: str | None = Field(default=None, description="Name of the placement")
    placement_cost_structure: str | None = Field(default=None, description="Placement Cost Structure")
    placement_id: str | None = Field(default=None, description="ID of the placement")
    placement_pixel_size: str | None = Field(default=None, description="Pixel size of the placement")
    placement_rate: str | None = Field(default=None, description="Placement Rate")
    placement_total_booked_units: str | None = Field(default=None, description="Placement Total Booked Units")
    placement_total_planned_media_cost: str | None = Field(
        default=None, description="Placement Total Planned Media Cost"
    )
    planned_media_cost: float | None = Field(default=None, description="Planned Media Cost")
    platform_type: str | None = Field(default=None, description="Platform Type")
    site_cm_360: str | None = Field(default=None, description="Name of the site in Campaign Manager 360")
    site_id_cm_360: str | None = Field(default=None, description="ID of the site in Campaign Manager 360")
    total_conversions: float | None = Field(default=None, description="Total Conversions")
    total_revenue: float | None = Field(default=None, description="Total Revenue")
    true_view_views: int | None = Field(default=None, description="True View Views")
    video_average_view_time: float | None = Field(default=None, description="Video Average View Time")
    video_companion_clicks: int | None = Field(default=None, description="Video Companion Clicks")
    video_completions: int | None = Field(default=None, description="Video Completions")
    video_first_quartile_completions: int | None = Field(default=None, description="Video First Quartile Completions")
    video_midpoints: int | None = Field(default=None, description="Video Midpoints")
    video_plays: int | None = Field(default=None, description="Video Plays")
    video_third_quartile_completions: int | None = Field(default=None, description="Video Third Quartile Completions")
    video_views: int | None = Field(default=None, description="Video Views")
    view_through_conversions: float | None = Field(default=None, description="View-Through Conversions")
    view_through_revenue: float | None = Field(default=None, description="View-Through Revenue")
