import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad set id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    account_id: str | None = Field(default=None, description="Advertiser the ad set belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad set name, from the ad report.")
    status: str | None = Field(default=None, description="NULL until an ad sets entity is collected.")
    budget: float | None = Field(default=None, description="NULL until an ad sets entity is collected.")
    budget_type: str | None = Field(default=None, description="NULL until an ad sets entity is collected.")
    start_date: dt.date | None = Field(default=None, description="NULL until an ad sets entity is collected.")
    end_date: dt.date | None = Field(default=None, description="NULL until an ad sets entity is collected.")
    created_at: dt.datetime | None = Field(default=None, description="NULL: Criteo reports no creation time.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL: Criteo reports no change time.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
