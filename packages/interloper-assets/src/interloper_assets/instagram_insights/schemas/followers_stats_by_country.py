import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FollowersStatsByCountry(Schema):
    """Instagram follower demographics by country. One row per segment per snapshot day, with the account's lifetime follower count in that segment (follower_demographics metric)."""

    country: str | None = Field(default=None, description="Country of the followers (ISO 3166-1 alpha-2 code).")
    follower_demographics: int | None = Field(
        default=None, description="Number of followers of the account in this segment."
    )
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
