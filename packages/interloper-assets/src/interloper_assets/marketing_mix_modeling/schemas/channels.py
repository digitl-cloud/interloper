import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Channels(Schema):
    date: dt.date = Field(..., description="The date of the campaign")
    channel_name: str = Field(..., description="The name of the channel")
    channel_type: str = Field(..., description="The type of the channel")
    channel_cost: float = Field(..., description="The cost of the channel")
    channel_impressions: int = Field(..., description="The impressions of the channel")
    channel_clicks: int = Field(..., description="The clicks of the channel")
    channel_conversions: int = Field(..., description="The conversions of the channel")
