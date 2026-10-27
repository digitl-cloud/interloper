import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAds(Schema):
    ad_id: str | None = Field(default=None, description="Ad id, the key the facts join on.")
    ad_group_id: str | None = Field(default=None, description="Ad set the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    account_id: str | None = Field(default=None, description="Advertiser the ad belongs to.")
    ad_name: str | None = Field(default=None, description="Ad name, from the ad report.")
    status: str | None = Field(default=None, description="NULL: Criteo reports no ad status.")
    created_at: dt.datetime | None = Field(default=None, description="NULL: Criteo reports no creation time.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL: Criteo reports no change time.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
