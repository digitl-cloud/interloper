import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Advertisers(Schema):
    """Criteo advertiser snapshot. One row per day: the source's own advertiser, as /advertisers/me lists it."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Advertiser id.")
    type: str | None = Field(default=None, description="Entity type Criteo names the resource with.")
    meta: str | None = Field(default=None, description="JSON of the application-specific metadata Criteo attaches.")
    advertiser_name: str | None = Field(default=None, description="Advertiser name.")
