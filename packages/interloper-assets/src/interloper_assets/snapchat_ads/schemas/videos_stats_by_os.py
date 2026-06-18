import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VideosStatsByOs(Schema):
    """The Ads by Country report provides insights into ad performance based on different countries. It includes key metrics such as attachment total view time, various conversion events (e.g., ad clicks, purchases, sign-ups), custom events, impressions, quartiles of ad views, saves, shares, spend, swipes, total installs, ad type, video views, and view completion rate."""

    id: str | None = Field(default=None, description="The unique ID of the Snapchat advertisement (maps to ad_id).")
    impressions: int | None = Field(default=None, description="The total number of times the ad was served.")
    operating_system: str | None = Field(default=None, description="The operating system of the device (e.g., ANDROID, IOS).")
    quartile_1: int | None = Field(default=None, description="Number of times the video was watched to 25% of its length.")
    quartile_2: int | None = Field(default=None, description="Number of times the video was watched to 50% of its length.")
    quartile_3: int | None = Field(default=None, description="Number of times the video was watched to 75% of its length.")
    saves: int | None = Field(default=None, description="The number of times the ad was saved.")
    screen_time_millis: int | None = Field(default=None, description="Total time in milliseconds the ad was on screen.")
    shares: int | None = Field(default=None, description="The number of times the ad was shared.")
    spend: int | None = Field(default=None, description="Total spend in micro-currency.")
    story_completes: int | None = Field(default=None, description="Number of times the story was watched to the end.")
    story_opens: int | None = Field(default=None, description="Number of times the story was opened.")
    swipe_up_percent: float | None = Field(default=None, description="The percentage of impressions that resulted in a swipe up.")
    swipes: int | None = Field(default=None, description="The total number of swipes on the ad.")
    total_impressions: int | None = Field(default=None, description="The total number of impressions including earned impressions.")
    total_installs: int | None = Field(default=None, description="Total number of app installs attributed to the ad.")
    uniques: int | None = Field(default=None, description="The number of unique users who saw the ad (Reach).")
    video_views: int | None = Field(default=None, description="The total number of video views (starts).")
    video_views_15s: int | None = Field(default=None, description="Number of times the video was watched for at least 15 seconds.")
    view_completion: int | None = Field(default=None, description="Number of times the video was watched to 100% completion.")
    view_time_millis: int | None = Field(default=None, description="Total time in milliseconds the video was viewed.")
    date: dt.date | None = Field(default=None, description="Partition date")
