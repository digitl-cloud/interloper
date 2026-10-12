import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimAds(Schema):
    """One row per Facebook ad with its ad set, from its latest entity row, else the facts."""

    ad_id: str | None = Field(default=None, description="Ad id, the key the facts join on.")
    ad_name: str | None = Field(default=None, description="Ad name.")
    adset_id: str | None = Field(default=None, description="Ad set the ad belongs to.")
    adset_name: str | None = Field(default=None, description="Ad set name, known from the facts.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    account_id: str | None = Field(default=None, description="Ad account the ad belongs to.")
    status: str | None = Field(default=None, description="Configured status, known from the entity asset only.")
    date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
