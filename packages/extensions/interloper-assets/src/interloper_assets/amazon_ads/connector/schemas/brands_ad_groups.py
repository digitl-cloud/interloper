import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class BrandsAdGroups(Schema):
    """Sponsored Brands ad group snapshot. One row per ad group and day, as the v4 ad groups list returns it with its extended data. Creation and update times arrive as epoch milliseconds and are stored as UTC instants."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_group_id: str | None = Field(default=None, description="Ad group id.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    name: str | None = Field(default=None, description="Ad group name.")
    state: str | None = Field(default=None, description="ENABLED, PAUSED, ARCHIVED or PROPOSED.")
    extended_data_creation_date: dt.datetime | None = Field(
        default=None, description="When the ad group was created, in UTC."
    )
    extended_data_last_update_date: dt.datetime | None = Field(
        default=None, description="When the ad group was last changed, in UTC."
    )
    extended_data_serving_status: str | None = Field(default=None, description="Serving status.")
    extended_data_serving_status_details: str | None = Field(
        default=None, description="JSON array of the serving status details."
    )
