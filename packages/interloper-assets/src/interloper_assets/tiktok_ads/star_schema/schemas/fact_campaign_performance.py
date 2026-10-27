import datetime as dt

from interloper import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in the advertiser's timezone.")
    account_id: str | None = Field(default=None, description="Advertiser id, the key the account dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the campaign dimension joins on.")
    impressions: int | None = Field(default=None, description="Times the ads were shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ads.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(
        default=None,
        description="NULL: TikTok counts each ad group's own optimization event, so there is no single primary "
        "conversion; see purchases.",
    )
    conversion_value: float | None = Field(default=None, description="NULL, see conversions.")
    purchases: int | None = Field(
        default=None, description="Website purchases (complete payment) under the advertiser's attribution setting."
    )
    revenue: float | None = Field(default=None, description="Value of the purchases, in the account currency.")
