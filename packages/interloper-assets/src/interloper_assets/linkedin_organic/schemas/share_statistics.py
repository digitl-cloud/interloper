import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ShareStatistics(Schema):
    """LinkedIn organization share (post) statistics including impressions, clicks, and engagement."""

    date: dt.date = Field(description="The date of the record")
    organizational_entity: str = Field(description="The organizational entity URN")
    click_count: int = Field(description="Total number of clicks")
    comment_count: int = Field(description="Total number of comments")
    engagement: float = Field(description="The engagement rate")
    impression_count: int = Field(description="Total number of impressions")
    like_count: int = Field(description="Total number of likes")
    share_count: int = Field(description="Total number of shares")
    unique_impressions_count: int = Field(description="Total number of unique impressions")
    time_range_end: int = Field(description="The end time of the time range (epoch ms)")
    time_range_start: int = Field(description="The start time of the time range (epoch ms)")
