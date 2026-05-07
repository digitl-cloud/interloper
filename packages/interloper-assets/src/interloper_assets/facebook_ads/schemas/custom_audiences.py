from interloper.schema import Schema
from pydantic import Field


class CustomAudiences(Schema):
    """Facebook custom audiences with approximate size bounds."""

    id: str = Field(description="The ID of the custom audience")
    name: str = Field(description="The name of the custom audience")
    description: str | None = Field(default=None, description="The description of the custom audience")
    account_id: str = Field(description="The ID of the Facebook account")
    approximate_count_lower_bound: int | None = Field(default=None, description="The lower bound of the approximate count")
    approximate_count_upper_bound: int | None = Field(default=None, description="The upper bound of the approximate count")
