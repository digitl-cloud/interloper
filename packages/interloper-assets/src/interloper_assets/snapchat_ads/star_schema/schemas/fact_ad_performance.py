import datetime as dt

from interloper import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in the account's timezone.")
    account_id: str | None = Field(default=None, description="Ad account id.")
    ad_id: str | None = Field(
        default=None, description="Ad id; the ad dimension gives its ad squad and campaign, which the report omits."
    )
    impressions: int | None = Field(default=None, description="Times the ad was shown.")
    clicks: int | None = Field(default=None, description="Swipe-ups on the ad, Snapchat's click.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(
        default=None, description="Currency of every amount in the row; NULL until an ad accounts entity is collected."
    )
    purchases: int | None = Field(
        default=None,
        description="Snapchat purchase conversions under its default attribution (28-day swipe, 1-day view).",
    )
    revenue: float | None = Field(
        default=None, description="Value of the purchase conversions, in the account currency."
    )
