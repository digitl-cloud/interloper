import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ConversionsStatsByTimeOfDay(Schema):
    """Conversion patterns by day of week and hour of day with revenue metrics."""

    conversions: int = Field(description="Number of conversions")
    date: dt.date | None = Field(
        default=None, description="The day the report was requested for (stamped from the partition)."
    )
    day: str = Field(description="Day of the week")
    hour: int = Field(description="Hour of the day")
    revenue: float = Field(description="Revenue generated")
