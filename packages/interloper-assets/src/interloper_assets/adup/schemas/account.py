import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Account(Schema):
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    username: str | None = Field(default=None, description="The username of the account")
    label: str | None = Field(default=None, description="The label of the account")
    type: str | None = Field(default=None, description="The type of the account")
    status: str | None = Field(default=None, description="The status of the account")
    language: str | None = Field(default=None, description="The language used in the account")
    timezone: str | None = Field(default=None, description="The timezone of the account")
    id: int | None = Field(default=None, description="The advertiser account ID")
    advertiser_url: str | None = Field(default=None, description="The advertiser's URL")
    channel_id: int | None = Field(default=None, description="The channel ID")

