import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad group id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    account_id: str | None = Field(default=None, description="Account the ad group belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad group name.")
    status: str | None = Field(default=None, description="Ad group status as the latest report states it.")
    budget: float | None = Field(default=None, description="NULL: Bing budgets the campaign.")
    budget_type: str | None = Field(default=None, description="NULL: Bing budgets the campaign.")
    start_date: dt.date | None = Field(default=None, description="NULL until an ad groups entity is collected.")
    end_date: dt.date | None = Field(default=None, description="NULL until an ad groups entity is collected.")
    created_at: dt.datetime | None = Field(default=None, description="NULL until an ad groups entity is collected.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL until an ad groups entity is collected.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
