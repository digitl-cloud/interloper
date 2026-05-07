import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Spendings(Schema):
    date: dt.date = Field(..., description="The date of the spending")
    channel_name: str = Field(..., description="The name of the channel")
    channel_type: str = Field(..., description="The type of the channel")
    channel_cost: float = Field(..., description="The cost of the channel")
