from interloper.schema import Schema
from pydantic import Field


class Account(Schema):
    username: str = Field(..., description="The username of the account")
    label: str = Field(..., description="The label of the account")
    type: str = Field(..., description="The type of the account")
    status: str = Field(..., description="The status of the account")
    language: str = Field(..., description="The language used in the account")
    timezone: str = Field(..., description="The timezone of the account")
    id: int = Field(..., description="The advertiser account ID")
    advertiser_url: str = Field(..., description="The advertiser's URL")
    channel_id: int = Field(..., description="The channel ID")