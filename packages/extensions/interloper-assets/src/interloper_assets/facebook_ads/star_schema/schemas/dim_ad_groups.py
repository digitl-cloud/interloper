import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad set id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    account_id: str | None = Field(default=None, description="Ad account the ad set belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad set name.")
    status: str | None = Field(default=None, description="Configured status; entity rows only.")
    budget: float | None = Field(
        default=None,
        description="Daily budget when set, else lifetime budget, in the account currency; NULL for none, when "
        "the account's currency is unknown, or on rows not built from the entity.",
    )
    budget_type: str | None = Field(
        default=None, description="daily or lifetime, by which budget the ad set sets; entity rows only."
    )
    start_date: dt.date | None = Field(
        default=None, description="Scheduled start, in the account's timezone; entity rows only."
    )
    end_date: dt.date | None = Field(
        default=None, description="Scheduled end, in the account's timezone; NULL when open-ended; entity rows only."
    )
    created_at: dt.datetime | None = Field(default=None, description="When the ad set was created; entity rows only.")
    updated_at: dt.datetime | None = Field(
        default=None, description="When the ad set was last changed; entity rows only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
