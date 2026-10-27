import datetime as dt

from interloper import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    date: dt.date | None = Field(
        default=None,
        description="The report day, in Bing's Amsterdam-Berlin time zone, the zone the connector requests.",
    )
    account_id: str | None = Field(default=None, description="Account id, the key the account dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the campaign dimension joins on.")
    impressions: int | None = Field(default=None, description="Times the ads were shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ads.")
    cost: float | None = Field(default=None, description="Amount spent, in the account currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(
        default=None, description="Bing conversions: the conversion goals the advertiser counts in Conversions."
    )
    conversion_value: float | None = Field(default=None, description="Revenue of the conversions, in currency.")
    purchases: int | None = Field(
        default=None,
        description="NULL: Bing does not mark which conversion goals are purchases; see conversions.",
    )
    revenue: float | None = Field(default=None, description="NULL, see purchases.")
