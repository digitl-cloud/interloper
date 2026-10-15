import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FollowersStatsByCity(Schema):
    """Instagram follower demographics by city. One row per segment per snapshot day, with the account's lifetime follower count in that segment (follower_demographics metric)."""

    city: str | None = Field(default=None, description="City of the followers, as named by Instagram.")
    follower_demographics: int | None = Field(
        default=None, description="Number of followers of the account in this segment."
    )
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
