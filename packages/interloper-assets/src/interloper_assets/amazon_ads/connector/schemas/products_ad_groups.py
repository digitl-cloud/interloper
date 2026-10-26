import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ProductsAdGroups(Schema):
    """Sponsored Products ad group snapshot. One row per ad group and day, as the v3 ad groups list returns it with its extended data."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_group_id: str | None = Field(default=None, description="Ad group id.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    name: str | None = Field(default=None, description="Ad group name.")
    global_ad_group_id: str | None = Field(default=None, description="Id of the ad group across marketplaces.")
    default_bid: float | None = Field(default=None, description="Default bid, in the profile's currency.")
    state: str | None = Field(
        default=None, description="ENABLED, PAUSED, ARCHIVED, ENABLING, USER_DELETED, PROPOSED or OTHER."
    )
    extended_data_creation_date_time: dt.datetime | None = Field(
        default=None, description="When the ad group was created."
    )
    extended_data_last_update_date_time: dt.datetime | None = Field(
        default=None, description="When the ad group was last changed."
    )
    extended_data_serving_status: str | None = Field(default=None, description="Serving status.")
    extended_data_serving_status_details: str | None = Field(
        default=None, description="JSON array of the serving status details."
    )
