import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """SA360 ad-level performance metrics including impressions, clicks, cost, and CTR."""

    ad_id: int | None = Field(..., description="The ad ID")
    ad_type: str | None = Field(..., description="The ad type")
    ad_name: str | None = Field(..., description="The ad name")
    ad_group_ad_status: str | None = Field(..., description="The ad status")
    ad_group_name: str | None = Field(..., description="The ad group name")
    ad_group_id: int | None = Field(..., description="The ad group ID")
    campaign_name: str | None = Field(..., description="The campaign name")
    campaign_id: int | None = Field(..., description="The campaign ID")
    customer_id: int | None = Field(..., description="The customer ID")
    date: dt.date | None = Field(..., description="The report date")
    impressions: int | None = Field(..., description="The number of impressions")
    clicks: int | None = Field(..., description="The number of clicks")
    ctr: float | None = Field(..., description="The click-through rate")
    average_cpc: float | None = Field(..., description="The average cost per click")
    cost_micros: int | None = Field(..., description="The cost in micros")
