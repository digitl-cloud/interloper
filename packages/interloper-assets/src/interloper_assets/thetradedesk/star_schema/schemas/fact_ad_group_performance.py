import datetime as dt

from interloper import Schema
from pydantic import Field


class FactAdGroupPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in UTC.")
    account_id: str | None = Field(default=None, description="Partner id.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    ad_group_id: str | None = Field(default=None, description="Ad group id, the key the ad group dimension joins on.")
    impressions: int | None = Field(default=None, description="Times the ad group's ads were shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ad group's ads.")
    cost: float | None = Field(
        default=None, description="Advertiser cost, what the advertiser is billed, in the advertiser's currency."
    )
    currency: str | None = Field(default=None, description="Currency of every amount in the row, the advertiser's.")
