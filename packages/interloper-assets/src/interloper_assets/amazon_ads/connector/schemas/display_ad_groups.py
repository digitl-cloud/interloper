import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DisplayAdGroups(Schema):
    """Sponsored Display ad group snapshot. One row per ad group and day, as the extended ad groups list returns it. Ids are numeric, as Sponsored Display sends them. Creation and update times arrive as epoch milliseconds and are stored as UTC instants."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_group_id: int | None = Field(default=None, description="Ad group id.")
    campaign_id: int | None = Field(default=None, description="Campaign the ad group belongs to.")
    name: str | None = Field(default=None, description="Ad group name.")
    default_bid: float | None = Field(default=None, description="Default bid, in the profile's currency.")
    bid_optimization: str | None = Field(
        default=None, description="What bids optimise for: reach, clicks or conversions."
    )
    state: str | None = Field(default=None, description="enabled, paused or archived.")
    tactic: str | None = Field(default=None, description="Targeting tactic: T00020 (contextual) or T00030 (audiences).")
    creative_type: str | None = Field(default=None, description="IMAGE or VIDEO.")
    serving_status: str | None = Field(default=None, description="Serving status.")
    creation_date: dt.datetime | None = Field(default=None, description="When the ad group was created, in UTC.")
    last_updated_date: dt.datetime | None = Field(
        default=None, description="When the ad group was last changed, in UTC."
    )
