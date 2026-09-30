import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VideosStatsByTargeting(Schema):
    """Pinterest video ad performance per ad and day, broken down by app type and placement targeting."""

    date: dt.date | None = Field(default=None, description="The report day.")
    pin_id: str | None = Field(default=None, description="The ID of the organic Pin behind the ad.")
    ad_name: str | None = Field(default=None, description="The ad name.")
    pin_promotion_id: str | None = Field(default=None, description="The pin promotion (ad) ID.")
    ad_group_name: str | None = Field(default=None, description="The ad group name.")
    ad_group_id: str | None = Field(default=None, description="The ad group ID.")
    campaign_name: str | None = Field(default=None, description="The campaign name.")
    campaign_id: str | None = Field(default=None, description="The campaign ID.")
    ad_account_id: str | None = Field(default=None, description="The ad account ID.")
    engagement_1: int | None = Field(default=None, description="Paid engagements.")
    engagement_2: int | None = Field(default=None, description="Earned engagements.")
    engagement_rate: float | None = Field(default=None, description="Overall engagement rate.")
    paid_impression: int | None = Field(default=None, description="Paid impressions.")
    outbound_click_1: int | None = Field(default=None, description="Paid outbound clicks.")
    outbound_click_2: int | None = Field(default=None, description="Earned outbound clicks.")
    repin_1: int | None = Field(default=None, description="Paid saves (repins).")
    spend_in_micro_dollar: float | None = Field(default=None, description="Total spend in micro dollars.")
    video_3sec_views_1: int | None = Field(default=None, description="Paid video views of at least 3 seconds.")
    video_15sec_unique_views_1: int | None = Field(
        default=None, description="Paid unique video views of at least 15 seconds."
    )
    video_mrc_views_1: int | None = Field(default=None, description="Paid MRC-standard video views.")
    video_3sec_views_2: int | None = Field(default=None, description="Earned video views of at least 3 seconds.")
    video_15sec_unique_views_2: int | None = Field(
        default=None, description="Earned unique video views of at least 15 seconds."
    )
    video_p100_complete_2: int | None = Field(default=None, description="Earned video views reaching 100% of length.")
    video_p0_combined_2: int | None = Field(default=None, description="Earned video starts.")
    video_p25_combined_2: int | None = Field(default=None, description="Video views reaching 25% of length (earned).")
    video_p50_combined_2: int | None = Field(default=None, description="Video views reaching 50% of length (earned).")
    video_p75_combined_2: int | None = Field(default=None, description="Video views reaching 75% of length (earned).")
    video_p95_combined_2: int | None = Field(default=None, description="Video views reaching 95% of length (earned).")
    video_mrc_views_2: int | None = Field(default=None, description="Earned MRC-standard video views.")
    paid_video_viewable_rate: float | None = Field(
        default=None, description="Share of paid video impressions that were viewable."
    )
    video_length: float | None = Field(default=None, description="The video length.")
    video_spend_in_dollar: float | None = Field(default=None, description="Spend on video ads, in dollars.")
    total_video_avg_watchtime_in_second: float | None = Field(
        default=None, description="Average video watch time in seconds (paid and earned)."
    )
    total_engagement: int | None = Field(default=None, description="Total engagements (paid and earned).")
    total_impression: int | None = Field(default=None, description="Total impressions (paid and earned).")
    total_clickthrough: int | None = Field(default=None, description="Total Pin clicks (paid and earned).")
    total_video_3sec_views: int | None = Field(
        default=None, description="Total video views of at least 3 seconds (paid and earned)."
    )
    total_video_15sec_unique_views: int | None = Field(
        default=None, description="Total unique video views of at least 15 seconds (paid and earned)."
    )
    total_video_p100_complete: int | None = Field(
        default=None, description="Total video views reaching 100% of length (paid and earned)."
    )
    total_video_p0_combined: int | None = Field(default=None, description="Total video starts (paid and earned).")
    total_video_p25_combined: int | None = Field(
        default=None, description="Video views reaching 25% of length (paid and earned)."
    )
    total_video_p50_combined: int | None = Field(
        default=None, description="Video views reaching 50% of length (paid and earned)."
    )
    total_video_p75_combined: int | None = Field(
        default=None, description="Video views reaching 75% of length (paid and earned)."
    )
    total_video_p95_combined: int | None = Field(
        default=None, description="Video views reaching 95% of length (paid and earned)."
    )
    total_video_mrc_views: int | None = Field(
        default=None, description="Total MRC-standard video views (paid and earned)."
    )
    total_repin_rate: float | None = Field(default=None, description="Total save (repin) rate.")
    targeting_type: str | None = Field(
        default=None, description="The targeting dimension this row is broken down by (APPTYPE or PLACEMENT)."
    )
    targeting_value: str | None = Field(default=None, description="The value of the targeting dimension for this row.")
