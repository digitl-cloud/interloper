import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FollowersStats(Schema):
    """LinkedIn organization page lifetime follower counts. One row per organization and day, each facet holding the top 100 segments with their organic and paid follower counts."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    organizational_entity: str | None = Field(default=None, description="The URN of the organization.")
    follower_counts_by_association_type: str | None = Field(
        default=None,
        description="Follower counts by association type (e.g. employees) (JSON array of segment and followerCounts).",
    )
    follower_counts_by_function: str | None = Field(
        default=None, description="Follower counts by job function (JSON array of segment and followerCounts)."
    )
    follower_counts_by_geo: str | None = Field(
        default=None, description="Follower counts by market area (JSON array of segment and followerCounts)."
    )
    follower_counts_by_geo_country: str | None = Field(
        default=None, description="Follower counts by country or region (JSON array of segment and followerCounts)."
    )
    follower_counts_by_industry: str | None = Field(
        default=None, description="Follower counts by industry (JSON array of segment and followerCounts)."
    )
    follower_counts_by_seniority: str | None = Field(
        default=None, description="Follower counts by seniority (JSON array of segment and followerCounts)."
    )
    follower_counts_by_staff_count_range: str | None = Field(
        default=None,
        description="Follower counts by staff count range of the followers' current organization (JSON array of segment and followerCounts).",
    )
