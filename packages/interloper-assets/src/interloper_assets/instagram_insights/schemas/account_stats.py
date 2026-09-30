import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AccountStats(Schema):
    """Daily Instagram account insights. One row per account per day with the account's new followers and reach."""

    end_time: dt.datetime | None = Field(
        default=None, description="End of the daily period the values cover, as returned by the API."
    )
    follower_count: int | None = Field(
        default=None,
        description="Number of new followers of the account on the day. Only available for the last 30 days, null beyond.",
    )
    reach: int | None = Field(
        default=None, description="Number of unique accounts that saw any of the account's content on the day."
    )
    date: dt.date | None = Field(
        default=None, description="The day the values were requested for (stamped from the partition)."
    )
