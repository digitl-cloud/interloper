import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Ad account the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name; entity rows only.")
    status: str | None = Field(default=None, description="Configured status (ACTIVE, PAUSED); entity rows only.")
    objective: str | None = Field(default=None, description="Campaign objective; entity rows only.")
    campaign_type: str | None = Field(default=None, description="Buy model (AUCTION, RESERVED); entity rows only.")
    budget: float | None = Field(
        default=None, description="Lifetime spend cap, in the account currency; NULL when uncapped; entity rows only."
    )
    budget_type: str | None = Field(
        default=None, description="lifetime when the campaign caps its spend, else NULL; entity rows only."
    )
    start_date: dt.date | None = Field(
        default=None,
        description="Scheduled start, in the account's timezone; entity rows only, and NULL until the account's "
        "timezone is known.",
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Scheduled end, in the account's timezone; NULL when open-ended; entity rows only, and NULL "
        "until the account's timezone is known.",
    )
    created_at: dt.datetime | None = Field(default=None, description="When the campaign was created; entity rows only.")
    updated_at: dt.datetime | None = Field(
        default=None, description="When the campaign was last changed; entity rows only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
