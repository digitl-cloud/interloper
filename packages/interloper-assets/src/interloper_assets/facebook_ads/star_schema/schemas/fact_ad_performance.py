import datetime as dt

from interloper import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in the account's timezone.")
    account_id: str | None = Field(default=None, description="Ad account id.")
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    ad_group_id: str | None = Field(default=None, description="Ad set id.")
    ad_id: str | None = Field(default=None, description="Ad id.")
    publisher_platform: str | None = Field(
        default=None, description="Platform the ad was shown on (facebook, instagram, ...)."
    )
    platform_position: str | None = Field(default=None, description="Placement on the platform (feed, story, ...).")
    impression_device: str | None = Field(default=None, description="Device the ad was shown on.")
    impressions: int | None = Field(default=None, description="Times the ad was shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ad.")
    reach: int | None = Field(default=None, description="People who saw the ad; not additive across days.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    purchases: int | None = Field(
        default=None,
        description="Facebook purchase actions under the account's attribution setting "
        "(7-day click, 1-day view by default).",
    )
    revenue: float | None = Field(default=None, description="Value of the purchase actions, in the account currency.")
