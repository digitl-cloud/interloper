import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignGroups(Schema):
    """LinkedIn campaign group snapshot. One row per campaign group and day, with its status and run schedule."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: int | None = Field(default=None, description="The unique identifier of the campaign group.")
    name: str | None = Field(default=None, description="The name of the campaign group.")
    status: str | None = Field(
        default=None, description="The status of the campaign group (e.g. ACTIVE, PAUSED, ARCHIVED)."
    )
    run_schedule_start: dt.datetime | None = Field(
        default=None, description="When the campaign group's run schedule starts (UTC)."
    )
    run_schedule_end: dt.datetime | None = Field(
        default=None, description="When the campaign group's run schedule ends (UTC); empty for an open-ended schedule."
    )
