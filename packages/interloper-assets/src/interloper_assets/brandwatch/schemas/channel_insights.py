import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ChannelInsights(Schema):
    """Brandwatch channel insights with social media engagement metrics."""

    channel_id: str = Field(description="The channel identifier")
    date: dt.date = Field(description="The date of the record")
