import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Interaction(Schema):
    """Usercentrics interaction-level consent analytics data."""

    date: dt.date = Field(description="The date of the record")
