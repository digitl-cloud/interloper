import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsByCountry(Schema):
    """Ad performance report segmented by country with audience metrics."""

    ad_id: int = Field(description="Unique identifier for the ad")
    ad_name: str = Field(description="Name of the ad")
    campaign_id: int = Field(description="Unique identifier for the campaign")
    campaign_name: str = Field(description="Name of the campaign")
    country_code: str = Field(description="Country code")
    spend: float = Field(description="Amount spent on the ad")
    cpc: float = Field(description="Cost per click")
    cpm: float = Field(description="Cost per thousand impressions")
    impressions: int = Field(description="Number of impressions")
    gross_impressions: int = Field(description="Total number of impressions")
    clicks: int = Field(description="Number of clicks on the ad")
    ctr: float = Field(description="Click-through rate")
    conversion: int = Field(description="Conversion value of the ad")
    cost_per_conversion: float = Field(description="Cost per conversion")
    conversion_rate: float = Field(description="Conversion rate")
    real_time_conversion: int = Field(description="Real-time conversion value")
    real_time_cost_per_conversion: float = Field(description="Real-time cost per conversion")
    real_time_conversion_rate: float = Field(description="Real-time conversion rate")
    result: int = Field(description="Result value of the ad")
    cost_per_result: float = Field(description="Cost per result")
    result_rate: float = Field(description="Result rate")
    real_time_result: int = Field(description="Real-time result value")
    real_time_cost_per_result: float = Field(description="Real-time cost per result")
    real_time_result_rate: float = Field(description="Real-time result rate")
    currency: str = Field(description="Currency used for the ad")
    stat_time_day: dt.date = Field(description="Day of the statistics")
