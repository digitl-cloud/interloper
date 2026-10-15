import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsStatsByAgeGender(Schema):
    """The Ads by Age Gender report provides insights into the performance of ads based on age and gender demographics. It includes key metrics such as clicks, gender, impressions, spend, and engagement rates."""

    account_currency: str | None = Field(default=None, description="The currency used for the account")
    account_id: str | None = Field(default=None, description="The ID of the account")
    account_name: str | None = Field(default=None, description="The name of the account")
    ad_id: str | None = Field(default=None, description="The ID of the ad")
    ad_name: str | None = Field(default=None, description="The name of the ad")
    adset_id: str | None = Field(default=None, description="The ID of the ad set")
    adset_name: str | None = Field(default=None, description="The name of the ad set")
    age: str | None = Field(default=None, description="The age range of the target audience")
    campaign_id: str | None = Field(default=None, description="The ID of the campaign")
    campaign_name: str | None = Field(default=None, description="The name of the campaign")
    clicks: int | None = Field(default=None, description="The number of clicks")
    cpc: float | None = Field(default=None, description="The cost per click")
    cpm: float | None = Field(default=None, description="The cost per thousand impressions")
    cpp: float | None = Field(default=None, description="The cost per purchase")
    ctr: float | None = Field(default=None, description="The click-through rate")
    date_start: dt.date | None = Field(default=None, description="The start date of the data")
    date_stop: dt.date | None = Field(default=None, description="The end date of the data")
    gender: str | None = Field(default=None, description="The gender of the target audience")
    impressions: int | None = Field(default=None, description="The number of impressions")
    reach: int | None = Field(default=None, description="The number of unique users reached")
    spend: float | None = Field(default=None, description="The amount spent")
