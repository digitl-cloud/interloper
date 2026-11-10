import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad group id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    account_id: str | None = Field(default=None, description="Account the ad group belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad group name.")
    status: str | None = Field(
        default=None, description="Configured ad group status, as the snapshot, else the latest report, states it."
    )
    start_date: dt.date | None = Field(
        default=None,
        description="Day the ad group starts serving, as Bing schedules it; NULL on rows built from the report or the facts.",
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Last day the ad group serves, NULL when it runs on or on rows built from the report or the facts.",
    )
    updated_at: dt.datetime | None = Field(
        default=None, description="When the ad group last changed; NULL on rows built from the report or the facts."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
