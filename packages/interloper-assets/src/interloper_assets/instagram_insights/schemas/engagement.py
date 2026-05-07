import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Engagement(Schema):
    """Instagram engagement metrics including interactions, likes, comments, shares, and saves."""

    date: dt.date = Field(description="The date of the record")
    accounts_engaged: int = Field(description="Number of accounts that interacted with content")
    total_interactions: int = Field(description="Total post, story, reel, video and live video interactions")
    follows_and_unfollows: int = Field(description="Number of accounts that followed or unfollowed")
    likes: int = Field(description="Number of likes minus unlikes")
    comments: int = Field(description="Number of comments minus deleted comments")
    shares: int = Field(description="Number of shares of posts, stories, reels, videos and live videos")
    saves: int = Field(description="Number of saves minus unsaves")
    replies: int = Field(description="Number of replies received from stories")
    profile_links_taps: int = Field(description="Number of taps on business address, call, email and text buttons")
    views: int = Field(description="Number of views of content for 3 seconds or more")
