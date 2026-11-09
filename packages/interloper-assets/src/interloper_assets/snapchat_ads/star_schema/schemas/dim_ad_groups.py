import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad squad id, the key the ad dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad squad belongs to.")
    account_id: str | None = Field(default=None, description="Ad account the ad squad belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad squad name.")
    status: str | None = Field(default=None, description="Configured status (ACTIVE, PAUSED).")
    budget: float | None = Field(
        default=None, description="Daily budget when set, else lifetime budget, in the account currency; NULL for none."
    )
    budget_type: str | None = Field(default=None, description="daily or lifetime, by which budget is published.")
    start_date: dt.date | None = Field(
        default=None, description="Scheduled start, in the account's timezone; NULL until the timezone is known."
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Scheduled end, in the account's timezone; NULL when open-ended or until the timezone is known.",
    )
    created_at: dt.datetime | None = Field(default=None, description="When the ad squad was created.")
    updated_at: dt.datetime | None = Field(default=None, description="When the ad squad was last changed.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
