import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Profiles(Schema):
    """Instagram account profile snapshot. One row per account per day with its follower, following and media counts."""

    id: str | None = Field(default=None, description="ID of the Instagram user.")
    followers_count: int | None = Field(default=None, description="Number of followers of the account.")
    follows_count: int | None = Field(default=None, description="Number of accounts the account follows.")
    media_count: int | None = Field(default=None, description="Number of media items published by the account.")
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
