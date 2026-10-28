import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad set id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    account_id: str | None = Field(default=None, description="Advertiser the ad set belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad set name, from the ad report.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
