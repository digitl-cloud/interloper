import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Account the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    status: str | None = Field(default=None, description="Campaign status as the latest report states it.")
    campaign_type: str | None = Field(default=None, description="Campaign type (Search, Performance max...).")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
