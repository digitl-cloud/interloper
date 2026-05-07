from interloper.schema import Schema
from pydantic import Field


class ConversionsByTimeOfDay(Schema):
    """Conversion patterns by day of week and hour of day with revenue metrics."""

    conversions: int = Field(description="Number of conversions")
    day: str = Field(description="Day of the week")
    hour: int = Field(description="Hour of the day")
    revenue: float = Field(description="Revenue generated")
