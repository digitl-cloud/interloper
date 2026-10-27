import datetime as dt

from interloper import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in the advertiser's timezone.")
    account_id: str | None = Field(default=None, description="Advertiser id.")
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    ad_group_id: str | None = Field(default=None, description="Ad group id.")
    ad_id: str | None = Field(default=None, description="Ad id.")
    impressions: int | None = Field(default=None, description="Times the ad was shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ad.")
    reach: int | None = Field(default=None, description="People who saw the ad; not additive across days.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(default=None, description="NULL, see the campaign fact.")
    conversion_value: float | None = Field(default=None, description="NULL, see the campaign fact.")
    purchases: int | None = Field(
        default=None, description="Website purchases (complete payment) under the advertiser's attribution setting."
    )
    revenue: float | None = Field(default=None, description="Value of the purchases, in the account currency.")
