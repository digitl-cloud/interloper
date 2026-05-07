import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VideosByPlatform(Schema):
    """Video ad performance report segmented by platform with engagement metrics."""

    ad_id: int = Field(description="Unique identifier for the ad")
    ad_name: str = Field(description="Name of the ad")
    ad_text: str = Field(description="Creative text or caption used in the ad")
    adgroup_id: int = Field(description="Unique identifier for the ad group")
    adgroup_name: str = Field(description="Name of the ad group")
    campaign_id: int = Field(description="Unique identifier for the campaign")
    campaign_name: str = Field(description="Name of the campaign")
    platform: str = Field(description="Device platform or OS")
    spend: float = Field(description="Total amount spent on the ad")
    impressions: int = Field(description="Total number of times the ad was shown")
    gross_impressions: int = Field(description="Total impressions including invalid/duplicate views")
    clicks: int = Field(description="Total number of clicks on the ad")
    ctr: float = Field(description="Click-through rate")
    reach: int = Field(description="Total number of unique users who saw the ad")
    frequency: float = Field(description="Average number of times each unique user saw the ad")
    currency: str = Field(description="Currency used for spend metrics")
    follows: int = Field(description="Number of new followers gained from the ad")
    likes: int = Field(description="Number of likes the ad received")
    shares: int = Field(description="Number of times the ad was shared")
    comments: int = Field(description="Number of comments the ad received")
    profile_visits_rate: float = Field(description="Profile visits divided by impressions")
    profile_visits: int = Field(description="Number of clicks on profile icon or username")
    clicks_on_music_disc: int = Field(description="Number of clicks on the music disc icon")
    video_play_actions: int = Field(description="Number of times the video started playing")
    video_watched_2s: int = Field(description="Video watched for at least 2 seconds")
    video_watched_6s: int = Field(description="Video watched for at least 6 seconds")
    average_video_play: float = Field(description="Average time the video was played per play")
    average_video_play_per_user: float = Field(description="Average time the video was played per unique user")
    video_views_p25: int = Field(description="Video watched to 25% of its length")
    video_views_p50: int = Field(description="Video watched to 50% of its length")
    video_views_p75: int = Field(description="Video watched to 75% of its length")
    video_views_p100: int = Field(description="Video watched to 100% completion")
    engaged_view: int = Field(description="Number of engaged views")
    engaged_view_15s: int = Field(description="Number of engaged views lasting at least 15 seconds")
    stat_time_day: dt.date = Field(description="Date for the statistics")
