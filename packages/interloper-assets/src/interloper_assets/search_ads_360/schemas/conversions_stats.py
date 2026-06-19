import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ConversionsStats(Schema):
    """SA360 conversion action metrics including conversion counts and values."""

    conversion_action_name: str | None = Field(..., description="The conversion action name")
    conversion_action_id: int | None = Field(..., description="The conversion action ID")
    conversion_action_category: str | None = Field(..., description="The conversion action category")
    conversion_action_status: str | None = Field(..., description="The conversion action status")
    conversion_action_type: str | None = Field(..., description="The conversion action type")
    campaign_name: str | None = Field(..., description="The campaign name")
    campaign_id: int | None = Field(..., description="The campaign ID")
    customer_id: int | None = Field(..., description="The customer ID")
    date: dt.date | None = Field(..., description="The report date")
    segments_conversion_action_name: str | None = Field(
        ..., description="The conversion action name segment"
    )
    conversions: float | None = Field(..., description="The number of conversions")
    conversions_value: float | None = Field(..., description="The total value of conversions")
    all_conversions: float | None = Field(..., description="The total number of all conversions")
    all_conversions_value: float | None = Field(..., description="The total value of all conversions")
    cross_device_conversions: float | None = Field(
        ..., description="The number of cross-device conversions"
    )
