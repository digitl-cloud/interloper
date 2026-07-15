import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class TwitterStats(Schema):
    """X (Twitter) channel performance metrics per day (Brandwatch/Falcon.io Measure)."""

    date: dt.date | None = Field(default=None, description="The date the metrics correspond to.")
    channel_id: str | None = Field(default=None, description="Unique identifier for the Facebook Page.")
    fans: int | None = Field(
        default=None, description="Total number of account followers (synonymous with 'followers')."
    )
    followers: int | None = Field(default=None, description="Total number of people following the account.")
    friends_count: int | None = Field(default=None, description="Total number of accounts the user is following.")
    net_fans: int | None = Field(default=None, description="Net change in fans/followers (New - Lost).")
    net_followers: int | None = Field(default=None, description="Net change in followers.")
    listed_count: int | None = Field(
        default=None, description="Total number of public lists this account is a member of."
    )
    listed_net: int | None = Field(
        default=None, description="Net change in the number of lists the account is added to."
    )
    statuses_count: int | None = Field(
        default=None, description="Total number of tweets (posts) published by the account."
    )
    statuses_net: int | None = Field(default=None, description="Net change in the number of tweets (posts) published.")
