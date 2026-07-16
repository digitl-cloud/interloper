import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CustomAudiences(Schema):
    """The Facebook Custom Audiences report provides information about custom audiences volume for a given account id"""

    account_id: str | None = Field(default=None, description="The ID of the Facebook account")
    approximate_count_lower_bound: int | None = Field(default=None, description="The lower bound of the approximate count of custom audiences")
    approximate_count_upper_bound: int | None = Field(default=None, description="The upper bound of the approximate count of custom audiences")
    description: str | None = Field(default=None, description="The description of the custom audience")
    id: str | None = Field(default=None, description="The ID of the custom audience")
    name: str | None = Field(default=None, description="The name of the custom audience")
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
