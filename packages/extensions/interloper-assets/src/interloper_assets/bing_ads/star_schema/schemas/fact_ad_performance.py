import datetime as dt

from interloper import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    date: dt.date | None = Field(
        default=None,
        description=(
            "The report day, in the campaign's Bing time zone: its account's (dim_accounts.timezone) "
            "unless the campaign was created in another."
        ),
    )
    account_id: str | None = Field(default=None, description="Account id.")
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    ad_group_id: str | None = Field(default=None, description="Ad group id.")
    ad_id: str | None = Field(default=None, description="Ad id.")
    impressions: int | None = Field(default=None, description="Times the ad was shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ad.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(default=None, description="Bing conversions, see the campaign fact.")
    conversion_value: float | None = Field(default=None, description="Revenue of the conversions, in currency.")
