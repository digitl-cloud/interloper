import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class GranularStats(Schema):
    """Usercentrics granular consent analytics data."""

    date: dt.date = Field(description="The date of the record")
