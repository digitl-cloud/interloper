import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """SA360 campaign performance metrics including impressions, clicks, cost, and CTR."""

    campaign_name: str | None = Field(..., description="The campaign name")
    campaign_id: int | None = Field(..., description="The campaign ID")
    campaign_status: str | None = Field(..., description="The campaign status")
    campaign_advertising_channel_type: str | None = Field(
        ..., description="The campaign advertising channel type"
    )
    campaign_bidding_strategy_type: str | None = Field(
        ..., description="The campaign bidding strategy type"
    )
    customer_id: int | None = Field(..., description="The customer ID")
    customer_account_type: str | None = Field(..., description="The customer account type")
    date: dt.date | None = Field(..., description="The report date")
    impressions: int | None = Field(..., description="The number of impressions")
    average_cost: float | None = Field(..., description="The average cost")
    clicks: int | None = Field(..., description="The number of clicks")
    ctr: float | None = Field(..., description="The click-through rate")
    average_cpc: float | None = Field(..., description="The average cost per click")
    cost_micros: int | None = Field(..., description="The cost in micros")
