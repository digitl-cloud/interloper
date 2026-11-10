import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad group id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    account_id: str | None = Field(default=None, description="Advertiser the ad group belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad group name.")
    status: str | None = Field(default=None, description="Configured status (ENABLE, DISABLE); entity rows only.")
    budget: float | None = Field(default=None, description="Budget, in the account currency; entity rows only.")
    budget_type: str | None = Field(default=None, description="Budget mode; entity rows only.")
    start_date: dt.date | None = Field(
        default=None,
        description="Scheduled start, in the advertiser's timezone; entity rows only, and NULL until the "
        "advertiser's timezone is known.",
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Scheduled end, in the advertiser's timezone; entity rows only, and NULL until the "
        "advertiser's timezone is known.",
    )
    created_at: dt.datetime | None = Field(default=None, description="When the ad group was created; entity rows only.")
    updated_at: dt.datetime | None = Field(
        default=None, description="When the ad group was last changed; entity rows only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    effective_status: str | None = Field(
        default=None, description="Status after TikTok's own checks (secondary status); entity rows only."
    )
