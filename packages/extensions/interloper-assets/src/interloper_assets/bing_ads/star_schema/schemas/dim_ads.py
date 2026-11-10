import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAds(Schema):
    ad_id: str | None = Field(default=None, description="Ad id, the key the facts join on.")
    ad_group_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign_id: str | None = Field(
        default=None, description="Campaign the ad belongs to; on a snapshot row, its ad group's campaign."
    )
    account_id: str | None = Field(default=None, description="Account the ad belongs to.")
    status: str | None = Field(
        default=None, description="Configured ad status, as the snapshot, else the latest report, states it."
    )
    updated_at: dt.datetime | None = Field(
        default=None, description="When the ad last changed; NULL on rows built from the report or the facts."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    ad_type: str | None = Field(
        default=None,
        description="Ad type, as the snapshot names it (Responsive Search Ad...), else as the report labels it.",
    )
    headline: str | None = Field(
        default=None,
        description="The headline of a responsive ad, as the snapshot, else the latest report, states it.",
    )
