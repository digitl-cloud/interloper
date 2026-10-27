import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Advertiser the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    status: str | None = Field(default=None, description="Configured status (ENABLE, DISABLE); entity rows only.")
    objective: str | None = Field(default=None, description="Objective type; entity rows only.")
    campaign_type: str | None = Field(default=None, description="Campaign type; entity rows only.")
    budget: float | None = Field(default=None, description="Budget, in the account currency; entity rows only.")
    budget_type: str | None = Field(default=None, description="Budget mode; entity rows only.")
    start_date: dt.date | None = Field(default=None, description="NULL: TikTok schedules the ad group.")
    end_date: dt.date | None = Field(default=None, description="NULL: TikTok schedules the ad group.")
    created_at: dt.datetime | None = Field(default=None, description="When the campaign was created; entity rows only.")
    updated_at: dt.datetime | None = Field(
        default=None, description="When the campaign was last changed; entity rows only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    effective_status: str | None = Field(
        default=None, description="Status after TikTok's own checks (secondary status); entity rows only."
    )
