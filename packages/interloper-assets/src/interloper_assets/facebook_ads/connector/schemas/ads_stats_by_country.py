import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsStatsByCountry(Schema):
    """The Ads by Country report provides insights into ad performance across different countries and regions. It includes key metrics such as clicks, country, impressions, spend, and engagement rates."""

    account_currency: str | None = Field(default=None, description="The currency used for the account.")
    account_id: str | None = Field(default=None, description="The ID of the account.")
    account_name: str | None = Field(default=None, description="The name of the account.")
    ad_id: str | None = Field(default=None, description="The ID of the ad.")
    ad_name: str | None = Field(default=None, description="The name of the ad.")
    adset_id: str | None = Field(default=None, description="The ID of the ad set.")
    adset_name: str | None = Field(default=None, description="The name of the ad set.")
    campaign_id: str | None = Field(default=None, description="The ID of the campaign.")
    campaign_name: str | None = Field(default=None, description="The name of the campaign.")
    clicks: int | None = Field(default=None, description="The number of clicks.")
    country: str | None = Field(default=None, description="The country where the data is from.")
    cpc: float | None = Field(default=None, description="The cost per click.")
    cpm: float | None = Field(default=None, description="The cost per thousand impressions.")
    cpp: float | None = Field(default=None, description="The cost per purchase.")
    ctr: float | None = Field(default=None, description="The click-through rate.")
    date_start: dt.date | None = Field(default=None, description="The start date of the data.")
    date_stop: dt.date | None = Field(default=None, description="The end date of the data.")
    impressions: int | None = Field(default=None, description="The number of impressions.")
    reach: int | None = Field(default=None, description="The number of unique users reached.")
    region: str | None = Field(default=None, description="The region where the data is from.")
    spend: float | None = Field(default=None, description="The amount spent.")
