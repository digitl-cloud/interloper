import datetime as dt

from interloper import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in the account's timezone.")
    account_id: str | None = Field(default=None, description="Ad account id, the key the account dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the campaign dimension joins on.")
    impressions: int | None = Field(default=None, description="Times the ads were shown.")
    clicks: int | None = Field(default=None, description="Swipe-ups on the ads, Snapchat's click.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(
        default=None, description="Currency of every amount in the row; NULL until an ad accounts entity is collected."
    )
    conversions: int | None = Field(
        default=None,
        description="NULL: Snapchat reports no single primary conversion, its results depend on the optimization "
        "goal; see purchases.",
    )
    conversion_value: float | None = Field(default=None, description="NULL, see conversions.")
    purchases: int | None = Field(
        default=None,
        description="Snapchat purchase conversions under its default attribution (28-day swipe, 1-day view).",
    )
    revenue: float | None = Field(
        default=None, description="Value of the purchase conversions, in the account currency."
    )
