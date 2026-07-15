import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class YoutubeStats(Schema):
    """YouTube channel performance metrics per day (Brandwatch/Falcon.io Measure)."""

    date: dt.date | None = Field(default=None, description="The date the metrics correspond to.")
    channel_id: str | None = Field(default=None, description="Unique identifier for the Facebook Page.")
    fans: int | None = Field(default=None, description="Total number of subscribers (synonymous with 'followers').")
    followers: int | None = Field(default=None, description="Total number of channel subscribers.")
    net_fans: int | None = Field(default=None, description="Net change in fans/subscribers (New - Lost).")
    net_followers: int | None = Field(default=None, description="Net change in channel subscribers.")
    video_views: int | None = Field(default=None, description="Total number of times videos were viewed.")
    video_view_time: float | None = Field(
        default=None, description="Total accumulated video view time, typically in minutes or seconds."
    )
    engagements: int | None = Field(
        default=None, description="Total number of all engagement actions (e.g., likes, dislikes, comments, shares)."
    )
    interactions: int | None = Field(default=None, description="A general count of interactions.")
    comments: int | None = Field(default=None, description="Total number of comments on videos.")
    likes: int | None = Field(default=None, description="Total number of 'Likes' on videos (Thumbs Up).")
    shares: int | None = Field(default=None, description="Total number of times videos were shared.")
    reactions: int | None = Field(
        default=None, description="Total count of all reactions (usually likes/dislikes/emojis)."
    )
    reactions_by_type_anger: int | None = Field(default=None, description="Number of 'Anger' reactions/emojis.")
    reactions_by_type_haha: int | None = Field(default=None, description="Number of 'Haha' reactions/emojis.")
    reactions_by_type_like: int | None = Field(default=None, description="Number of 'Like' reactions/emojis.")
    reactions_by_type_love: int | None = Field(default=None, description="Number of 'Love' reactions/emojis.")
    reactions_by_type_sorry: int | None = Field(default=None, description="Number of 'Sorry/Sad' reactions/emojis.")
    reactions_by_type_wow: int | None = Field(default=None, description="Number of 'Wow' reactions/emojis.")
