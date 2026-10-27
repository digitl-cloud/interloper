import datetime as dt

from interloper import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    date: dt.date | None = Field(
        default=None, description="The report day as Criteo reports it: a UTC day, Criteo's default report timezone."
    )
    account_id: str | None = Field(default=None, description="Advertiser id.")
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    ad_group_id: str | None = Field(default=None, description="Ad set id.")
    ad_id: str | None = Field(default=None, description="Ad id.")
    impressions: int | None = Field(default=None, description="Displays of the ad.")
    clicks: int | None = Field(default=None, description="Clicks on the ad.")
    cost: float | None = Field(default=None, description="Amount the advertiser spent, in currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(default=None, description="Criteo sales, see the campaign fact.")
    conversion_value: float | None = Field(default=None, description="Revenue of the sales, in currency.")
    purchases: int | None = Field(default=None, description="Criteo sales, see the campaign fact.")
    revenue: float | None = Field(default=None, description="Revenue of the sales, in currency.")
