import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Advertisers(Schema):
    """Criteo advertiser snapshot. One row per day: the source's own advertiser, as /advertisers/me lists it, with the currency and time zone its Marketplace Performance Outcomes record states. The reports are requested in that currency and time zone."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Advertiser id.")
    type: str | None = Field(default=None, description="Entity type Criteo names the resource with.")
    meta: str | None = Field(default=None, description="JSON of the application-specific metadata Criteo attaches.")
    advertiser_name: str | None = Field(default=None, description="Advertiser name.")
    currency_name: str | None = Field(
        default=None, description="ISO 4217 currency of the advertiser's bids and budgets."
    )
    time_zone_id: str | None = Field(
        default=None, description="IANA time zone of the advertiser; empty when Criteo states none."
    )
