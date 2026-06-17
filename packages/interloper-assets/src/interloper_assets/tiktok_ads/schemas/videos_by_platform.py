import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VideosByPlatform(Schema):
    """The Ads by Platform report provides insights into ad performance categorized by platform. It includes key metrics such as clicks, conversion value, conversion rate, cost per conversion, cost per result, cost per thousand impressions (CPM), click-through rate (CTR), currency used, total number of impressions, platform, and real-time metrics."""

    ad_id: int | None = Field(default=None, description="The unique ID of the TikTok advertisement.")
    ad_name: str | None = Field(default=None, description="The name of the TikTok advertisement.")
    ad_text: str | None = Field(default=None, description="The creative text or caption used in the ad.")
    adgroup_id: int | None = Field(default=None, description="The unique ID of the ad group.")
    adgroup_name: str | None = Field(default=None, description="The name of the ad group.")
    average_video_play: float | None = Field(default=None, description="Average time the video was played per video play (total play time / video plays).")
    average_video_play_per_user: float | None = Field(default=None, description="Average time the video was played per unique user.")
    campaign_id: int | None = Field(default=None, description="The unique ID of the campaign.")
    campaign_name: str | None = Field(default=None, description="The name of the campaign.")
    clicks: int | None = Field(default=None, description="Total number of clicks on the ad.")
    clicks_on_music_disc: int | None = Field(default=None, description="Number of clicks on the music disc icon in the ad.")
    comments: int | None = Field(default=None, description="Number of comments the ad received.")
    ctr: float | None = Field(default=None, description="Click-through rate (clicks / impressions).")
    currency: str | None = Field(default=None, description="The currency used for spend metrics.")
    engaged_view: int | None = Field(default=None, description="Number of engaged views (user stays for a certain duration or interacts).")
    engaged_view_15s: int | None = Field(default=None, description="Number of engaged views lasting at least 15 seconds.")
    follows: int | None = Field(default=None, description="Number of new followers gained from the ad.")
    frequency: float | None = Field(default=None, description="Average number of times each unique user saw the ad.")
    gross_impressions: int | None = Field(default=None, description="Total number of times the ad was served, including invalid/duplicate views.")
    impressions: int | None = Field(default=None, description="Total number of times the ad was shown to users.")
    likes: int | None = Field(default=None, description="Number of likes the ad received.")
    platform: str | None = Field(default=None, description="The device platform or OS (Android, iOS) where the ad was served.")
    profile_visits: int | None = Field(default=None, description="Number of clicks on the user's profile icon or username.")
    profile_visits_rate: float | None = Field(default=None, description="Profile visits divided by impressions.")
    reach: int | None = Field(default=None, description="Total number of unique users who saw the ad.")
    shares: int | None = Field(default=None, description="Number of times the ad was shared.")
    spend: float | None = Field(default=None, description="Total amount spent on the ad.")
    stat_time_day: dt.date | None = Field(default=None, description="The date for which the statistics are reported.")
    video_play_actions: int | None = Field(default=None, description="Number of times the video started playing.")
    video_views_p100: int | None = Field(default=None, description="Number of times the video was watched to 100% completion.")
    video_views_p25: int | None = Field(default=None, description="Number of times the video was watched to 25% of its length.")
    video_views_p50: int | None = Field(default=None, description="Number of times the video was watched to 50% of its length.")
    video_views_p75: int | None = Field(default=None, description="Number of times the video was watched to 75% of its length.")
    video_watched_2s: int | None = Field(default=None, description="Number of times the video was watched for at least 2 seconds.")
    video_watched_6s: int | None = Field(default=None, description="Number of times the video was watched for at least 6 seconds.")
    date: dt.date | None = Field(default=None, description="Partition date")
