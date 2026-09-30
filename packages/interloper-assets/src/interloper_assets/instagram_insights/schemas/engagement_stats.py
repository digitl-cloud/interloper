import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class EngagementStats(Schema):
    """Daily Instagram account engagement totals. One row per account per day, fetched from the account insights with metric_type=total_value."""

    reach: int | None = Field(
        default=None,
        description="The number of unique accounts that have seen your content, at least once, including in ads. Content includes posts, stories, reels, videos and live videos. This metric is estimated.",
    )
    accounts_engaged: int | None = Field(
        default=None,
        description="The number of accounts that have interacted with your content, including in ads. Content includes posts, stories, reels, videos and live videos. Interactions can include actions such as likes, saves, comments, shares or replies. This metric is estimated.",
    )
    total_interactions: int | None = Field(
        default=None,
        description="The total number of post interactions, story interactions, reels interactions, video interactions and live video interactions, including any interactions on boosted content.",
    )
    follows_and_unfollows: int | None = Field(
        default=None,
        description="The number of accounts that followed you and the number of accounts that unfollowed you or left Instagram in the selected time period. Not returned if the IG User has less than 100 followers.",
    )
    likes: int | None = Field(default=None, description="The number of likes on your posts, reels, and videos.")
    comments: int | None = Field(
        default=None,
        description="The number of comments on your posts, reels, videos and live videos. This metric is in development.",
    )
    shares: int | None = Field(
        default=None, description="The number of shares of your posts, stories, reels, videos and live videos."
    )
    saves: int | None = Field(default=None, description="The number of saves of your posts, reels, and videos.")
    replies: int | None = Field(
        default=None,
        description="The number of replies you received from your story, including text replies and quick reaction replies.",
    )
    profile_links_taps: int | None = Field(
        default=None,
        description="The number of taps on your business address, call button, email button and text button.",
    )
    views: int | None = Field(
        default=None,
        description="The number of times your content was played or displayed. Content includes reels, posts, stories. This metric is in development.",
    )
    reposts: int | None = Field(
        default=None, description="The number of reposts of your posts, stories, reels, and videos."
    )
    date: dt.date | None = Field(
        default=None, description="The day the values were requested for (stamped from the partition)."
    )
