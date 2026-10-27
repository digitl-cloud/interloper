import datetime as dt

from interloper import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    date: dt.date | None = Field(
        default=None, description="The report day as Criteo reports it: a UTC day, Criteo's default report timezone."
    )
    account_id: str | None = Field(default=None, description="Advertiser id, the key the account dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the campaign dimension joins on.")
    impressions: int | None = Field(default=None, description="Displays of the ads.")
    clicks: int | None = Field(default=None, description="Clicks on the ads.")
    cost: float | None = Field(default=None, description="Amount the advertiser spent, in currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(
        default=None,
        description="Criteo sales, its primary conversion, under the advertiser's own attribution setting.",
    )
    conversion_value: float | None = Field(default=None, description="Revenue of the sales, in currency.")
    purchases: int | None = Field(
        default=None, description="Criteo sales under the advertiser's own attribution setting; same as conversions."
    )
    revenue: float | None = Field(default=None, description="Revenue of the sales, in currency.")
