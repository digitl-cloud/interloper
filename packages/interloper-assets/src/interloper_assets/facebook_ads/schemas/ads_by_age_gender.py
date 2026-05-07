import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsByAgeGenderReport(Schema):
    """Facebook Ads performance broken down by age and gender demographics."""

    account_currency: str = Field(description="The currency used for the account")
    account_id: str = Field(description="The ID of the account")
    account_name: str = Field(description="The name of the account")
    ad_id: str = Field(description="The ID of the ad")
    ad_name: str = Field(description="The name of the ad")
    adset_id: str = Field(description="The ID of the ad set")
    adset_name: str = Field(description="The name of the ad set")
    age: str = Field(description="The age range of the target audience")
    campaign_id: str = Field(description="The ID of the campaign")
    campaign_name: str = Field(description="The name of the campaign")
    clicks: int = Field(description="The number of clicks")
    cpc: float | None = Field(default=None, description="The cost per click")
    cpm: float | None = Field(default=None, description="The cost per thousand impressions")
    cpp: float | None = Field(default=None, description="The cost per purchase")
    ctr: float | None = Field(default=None, description="The click-through rate")
    date_start: dt.date = Field(description="The start date of the data")
    date_stop: dt.date = Field(description="The end date of the data")
    gender: str = Field(description="The gender of the target audience")
    impressions: int = Field(description="The number of impressions")
    reach: int | None = Field(default=None, description="The number of unique users reached")
    spend: float = Field(description="The amount spent")
