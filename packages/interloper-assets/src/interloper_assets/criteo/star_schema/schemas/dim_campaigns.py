import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Advertiser the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name, from the campaign report.")
    status: str | None = Field(default=None, description="NULL: Criteo sets the status on the ad set.")
    objective: str | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    campaign_type: str | None = Field(default=None, description="NULL: Criteo has no campaign sub-product.")
    budget: float | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    budget_type: str | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    start_date: dt.date | None = Field(default=None, description="NULL: Criteo schedules the ad set.")
    end_date: dt.date | None = Field(default=None, description="NULL: Criteo schedules the ad set.")
    created_at: dt.datetime | None = Field(default=None, description="NULL: Criteo reports no creation time.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL: Criteo reports no change time.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
