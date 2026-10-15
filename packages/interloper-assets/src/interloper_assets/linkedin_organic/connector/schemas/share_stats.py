import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ShareStats(Schema):
    """LinkedIn organization organic post performance by day. One row per organization per day, aggregated over all its posts; sponsored activity is excluded."""

    date: dt.date | None = Field(
        default=None, description="The day the statistics cover (stamped from the partition, UTC)."
    )
    organizational_entity: str | None = Field(default=None, description="The URN of the organization.")
    time_range_start: dt.datetime | None = Field(default=None, description="Start of the covered day (UTC).")
    time_range_end: dt.datetime | None = Field(default=None, description="End of the covered day (UTC).")
    total_share_statistics_click_count: int | None = Field(
        default=None, description="Clicks on the organization's posts."
    )
    total_share_statistics_comment_count: int | None = Field(
        default=None, description="Comments on the organization's posts."
    )
    total_share_statistics_engagement: float | None = Field(
        default=None, description="Organic clicks, likes, comments and shares divided by impressions."
    )
    total_share_statistics_impression_count: int | None = Field(
        default=None, description="Impressions of the organization's posts."
    )
    total_share_statistics_like_count: int | None = Field(
        default=None,
        description="Likes on the organization's posts; can be negative when a sponsored like is later withdrawn.",
    )
    total_share_statistics_share_count: int | None = Field(
        default=None, description="Shares of the organization's posts, instant reposts excluded."
    )
    total_share_statistics_unique_impressions_count: int | None = Field(
        default=None, description="Unique impressions of the organization's posts."
    )
