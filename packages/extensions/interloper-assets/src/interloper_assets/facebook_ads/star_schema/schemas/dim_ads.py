import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAds(Schema):
    ad_id: str | None = Field(default=None, description="Ad id, the key the facts join on.")
    ad_group_id: str | None = Field(default=None, description="Ad set the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    account_id: str | None = Field(default=None, description="Ad account the ad belongs to.")
    ad_name: str | None = Field(default=None, description="Ad name.")
    status: str | None = Field(default=None, description="Configured status, known from the entity rows only.")
    created_at: dt.datetime | None = Field(default=None, description="When the ad was created; entity rows only.")
    updated_at: dt.datetime | None = Field(default=None, description="When the ad was last changed; entity rows only.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    effective_status: str | None = Field(
        default=None,
        description="Status after Facebook's own checks (ACTIVE, PAUSED, WITH_ISSUES...); entity rows only.",
    )
