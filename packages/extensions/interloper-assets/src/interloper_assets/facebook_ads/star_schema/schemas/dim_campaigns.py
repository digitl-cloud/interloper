import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Ad account the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    status: str | None = Field(default=None, description="Configured status, known from the entity rows only.")
    objective: str | None = Field(default=None, description="Campaign objective.")
    campaign_type: str | None = Field(default=None, description="Buying type (AUCTION, RESERVED), entity rows only.")
    budget_type: str | None = Field(
        default=None, description="daily or lifetime, by which budget the campaign sets; entity rows only."
    )
    start_date: dt.date | None = Field(
        default=None, description="Scheduled start, in the account's timezone; entity rows only."
    )
    end_date: dt.date | None = Field(
        default=None, description="Scheduled end, in the account's timezone; NULL when open-ended; entity rows only."
    )
    created_at: dt.datetime | None = Field(default=None, description="When the campaign was created; entity rows only.")
    updated_at: dt.datetime | None = Field(
        default=None, description="When the campaign was last changed; entity rows only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    effective_status: str | None = Field(
        default=None,
        description="Status after Facebook's own checks (ACTIVE, PAUSED, WITH_ISSUES...); entity rows only.",
    )
