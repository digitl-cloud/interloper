import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStatistics(Schema):
    """Criteo campaign-level performance statistics with attribution metrics."""

    day: dt.date = Field(description="The date of the record")
    campaign_id: str = Field(description="The campaign identifier")
    clicks: int = Field(description="Number of clicks")
    displays: int = Field(description="Number of displays/impressions")
    advertiser_cost: float = Field(description="Total advertiser cost")
    visits: int = Field(description="Number of visits")
    sales_all_client_attribution: float = Field(description="Sales with all client attribution")
    revenue_generated_all_client_attribution: float = Field(description="Revenue with all client attribution")
