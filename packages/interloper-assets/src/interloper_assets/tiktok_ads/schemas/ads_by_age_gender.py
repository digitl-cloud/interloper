import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsByAgeGender(Schema):
    """The Ads by Age Gender report provides insights into ad performance segmented by age and gender demographics. It includes key metrics such as clicks, conversions, conversion rate, cost per conversion, cost per result, cost per click (CPC), cost per thousand impressions (CPM), click-through rate (CTR), currency used, date of data, gross impressions, impressions, and real-time metrics."""

    ad_id: int | None = Field(default=None, description="Unique identifier for the ad")
    ad_name: str | None = Field(default=None, description="Name of the ad")
    ad_text: str | None = Field(default=None, description="Text content of the ad")
    adgroup_id: int | None = Field(default=None, description="Unique identifier for the adgroup")
    adgroup_name: str | None = Field(default=None, description="Name for the adgroup")
    age: str | None = Field(default=None, description="The age of the target audience")
    campaign_id: int | None = Field(default=None, description="Unique identifier for the campaign")
    campaign_name: str | None = Field(default=None, description="Name of the campaign")
    clicks: int | None = Field(default=None, description="Number of clicks on the ad")
    conversion: int | None = Field(default=None, description="Conversion value of the ad")
    conversion_rate: float | None = Field(default=None, description="Conversion rate of the ad")
    cost_per_1000_reached: float | None = Field(default=None, description="Cost per 1000 reached impressions")
    cost_per_conversion: float | None = Field(default=None, description="Cost per conversion")
    cost_per_result: float | None = Field(default=None, description="Cost per result")
    cost_per_secondary_goal_result: str | None = Field(default=None, description="Cost per secondary goal result")
    cpc: float | None = Field(default=None, description="Cost per thousand impressions")
    cpm: float | None = Field(default=None, description="Cost per thousand impressions")
    ctr: float | None = Field(default=None, description="Click-through rate")
    currency: str | None = Field(default=None, description="Currency used for the ad")
    frequency: float | None = Field(default=None, description="Frequency of the ad")
    gender: str | None = Field(default=None, description="The gender of the target audience")
    gross_impressions: int | None = Field(default=None, description="Total number of impressions")
    impressions: int | None = Field(default=None, description="Number of impressions")
    reach: int | None = Field(default=None, description="Number of unique users reached")
    real_time_conversion: int | None = Field(default=None, description="Real-time conversion value of the ad")
    real_time_conversion_rate: float | None = Field(default=None, description="Real-time conversion rate of the ad")
    real_time_cost_per_conversion: float | None = Field(default=None, description="Real-time cost per conversion")
    real_time_cost_per_result: float | None = Field(default=None, description="Real-time cost per result")
    real_time_result: int | None = Field(default=None, description="Real-time result value of the ad")
    real_time_result_rate: float | None = Field(default=None, description="Real-time result rate of the ad")
    result: int | None = Field(default=None, description="Result value of the ad")
    result_rate: float | None = Field(default=None, description="Result rate of the ad")
    secondary_goal_result: str | None = Field(default=None, description="Secondary goal result value of the ad")
    secondary_goal_result_rate: str | None = Field(default=None, description="Secondary goal result rate of the ad")
    spend: float | None = Field(default=None, description="Amount spent on the ad")
    stat_time_day: dt.date | None = Field(default=None, description="Day of the ad")
    date: dt.date | None = Field(default=None, description="Partition date")
