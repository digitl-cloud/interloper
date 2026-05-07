import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsReport(Schema):
    """Ad-level performance report with basic metrics including spend, clicks, conversions, and reach."""

    ad_id: int = Field(description="Unique identifier for the ad")
    ad_name: str = Field(description="Name of the ad")
    ad_text: str = Field(description="Text content of the ad")
    adgroup_id: int = Field(description="Unique identifier for the ad group")
    adgroup_name: str = Field(description="Name of the ad group")
    campaign_id: int = Field(description="Unique identifier for the campaign")
    campaign_name: str = Field(description="Name of the campaign")
    spend: float = Field(description="Amount spent on the ad")
    complete_payment: int = Field(description="Number of website purchase actions attributed to the ad")
    total_complete_payment_rate: float = Field(description="Total value of website purchase actions attributed to the ad")
    vta_complete_payment: int = Field(description="Purchase events attributed to viewing the ad")
    vta_complete_payment_value: float = Field(description="Value of purchase events attributed to viewing the ad")
    cpc: float = Field(description="Cost per click")
    cpm: float = Field(description="Cost per thousand impressions")
    impressions: int = Field(description="Number of impressions")
    gross_impressions: int = Field(description="Total number of impressions")
    clicks: int = Field(description="Number of clicks on the ad")
    ctr: float = Field(description="Click-through rate")
    reach: int = Field(description="Number of unique users reached")
    cost_per_1000_reached: float = Field(description="Cost per 1000 reached impressions")
    conversion: int = Field(description="Conversion value of the ad")
    cost_per_conversion: float = Field(description="Cost per conversion")
    conversion_rate: float = Field(description="Conversion rate of the ad")
    real_time_conversion: int = Field(description="Real-time conversion value")
    real_time_cost_per_conversion: float = Field(description="Real-time cost per conversion")
    real_time_conversion_rate: float = Field(description="Real-time conversion rate")
    result: int = Field(description="Result value of the ad")
    cost_per_result: float = Field(description="Cost per result")
    result_rate: float = Field(description="Result rate of the ad")
    real_time_result: int = Field(description="Real-time result value")
    real_time_cost_per_result: float = Field(description="Real-time cost per result")
    real_time_result_rate: float = Field(description="Real-time result rate")
    secondary_goal_result: str = Field(description="Secondary goal result value")
    cost_per_secondary_goal_result: str = Field(description="Cost per secondary goal result")
    secondary_goal_result_rate: str = Field(description="Secondary goal result rate")
    frequency: float = Field(description="Frequency of the ad")
    currency: str = Field(description="Currency used for the ad")
    stat_time_day: dt.date = Field(description="Day of the statistics")
