import datetime as dt

from interloper import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in UTC.")
    account_id: str | None = Field(default=None, description="Partner id, the key the account dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the campaign dimension joins on.")
    impressions: int | None = Field(default=None, description="Times the ads were shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ads.")
    cost: float | None = Field(
        default=None, description="Advertiser cost, what the advertiser is billed, in the advertiser's currency."
    )
    currency: str | None = Field(default=None, description="Currency of every amount in the row, the advertiser's.")
