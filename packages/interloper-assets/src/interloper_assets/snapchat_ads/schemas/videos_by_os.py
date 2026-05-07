from interloper.schema import Schema
from pydantic import Field


class VideosByOs(Schema):
    """Video ad performance report segmented by operating system."""

    id: str = Field(description="Unique identifier for the ad")
    operating_system: str = Field(description="Operating system of the device")
    impressions: int = Field(description="Total number of times the ad was served")
    swipes: int = Field(description="Total number of swipes on the ad")
    spend: int = Field(description="Total spend in micro-currency")
    video_views: int = Field(description="Total number of video views")
    quartile_1: int = Field(description="Video watched to 25% of its length")
    quartile_2: int = Field(description="Video watched to 50% of its length")
    quartile_3: int = Field(description="Video watched to 75% of its length")
    view_completion: int = Field(description="Video watched to 100% completion")
    view_time_millis: int = Field(description="Total time in milliseconds the video was viewed")
    screen_time_millis: int = Field(description="Total time in milliseconds the ad was on screen")
    shares: int = Field(description="Number of times the ad was shared")
    saves: int = Field(description="Number of times the ad was saved")
    story_completes: int = Field(description="Number of times the story was watched to the end")
    story_opens: int = Field(description="Number of times the story was opened")
    swipe_up_percent: float = Field(description="Percentage of impressions resulting in a swipe up")
    total_impressions: int = Field(description="Total impressions including earned impressions")
    total_installs: int = Field(description="Total app installs attributed to the ad")
    uniques: int = Field(description="Number of unique users who saw the ad")
    video_views_15s: int = Field(description="Video watched for at least 15 seconds")
