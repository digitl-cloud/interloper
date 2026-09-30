import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FollowersStatsByAgeGender(Schema):
    """Instagram follower demographics by gender and age bracket. One row per segment per snapshot day, with the account's lifetime follower count in that segment (follower_demographics metric)."""

    gender: str | None = Field(default=None, description="Gender of the followers (F, M or U).")
    age: str | None = Field(default=None, description="Age bracket of the followers (e.g. 18-24).")
    follower_demographics: int | None = Field(
        default=None, description="Number of followers of the account in this segment."
    )
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
