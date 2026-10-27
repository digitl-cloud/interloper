import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Account the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    status: str | None = Field(default=None, description="Campaign status as the latest report states it.")
    objective: str | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    campaign_type: str | None = Field(default=None, description="Campaign type (Search, Performance max...).")
    budget: float | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    budget_type: str | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    start_date: dt.date | None = Field(default=None, description="NULL: Bing schedules the ad group.")
    end_date: dt.date | None = Field(default=None, description="NULL: Bing schedules the ad group.")
    created_at: dt.datetime | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
