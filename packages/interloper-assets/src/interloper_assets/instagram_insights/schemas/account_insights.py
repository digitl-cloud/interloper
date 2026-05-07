import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AccountInsights(Schema):
    """Instagram account-level insights including follower count and reach."""

    date: dt.date = Field(description="The date of the record")
    follower_count: int = Field(description="Number of followers")
    reach: int = Field(description="Number of people reached")
