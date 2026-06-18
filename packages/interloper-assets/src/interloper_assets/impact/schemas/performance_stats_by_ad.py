import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PerformanceStatsByAd(Schema):
    """Performance report by ad"""

    action_cost: float | None = Field(default=None, description="Cost associated with actions from the ad")
    actions: int | None = Field(default=None, description="Number of actions taken from the ad")
    ad_end_date: dt.datetime | None = Field(default=None, description="End date of the ad campaign")
    ad_id: str | None = Field(default=None, description="Unique identifier for the ad")
    ad_name: str | None = Field(default=None, description="Name of the ad")
    ad_start_date: dt.datetime | None = Field(default=None, description="Start date of the ad campaign")
    ad_type: str | None = Field(default=None, description="Type or category of the ad")
    calls: int | None = Field(default=None, description="Number of calls generated from the ad")
    click_cost: float | None = Field(default=None, description="Cost associated with clicks on the ad")
    clicks: int | None = Field(default=None, description="Number of clicks the ad received")
    cpc: float | None = Field(default=None, description="Cost per click metric for the ad")
    cpc_cost: float | None = Field(default=None, description="Cost per click for the ad")
    impressions: int | None = Field(default=None, description="Number of impressions the ad received")
    landing_page: str | None = Field(default=None, description="URL of the landing page for the ad")
    media_count: int | None = Field(default=None, description="Count of media items associated with the ad")
    network: int | None = Field(default=None, description="Advertising network through which the ad is served")
    other_cost: float | None = Field(default=None, description="Other costs associated with the ad")
    revenue: float | None = Field(default=None, description="Revenue generated from the ad")
    total_cost: float | None = Field(default=None, description="Total cost incurred for the ad")
    date: dt.date | None = Field(default=None, description="Partition date")
