import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """LinkedIn ad account snapshot. One row per ad account and day, with its name, currency and status."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: int | None = Field(default=None, description="The unique identifier of the ad account.")
    name: str | None = Field(default=None, description="The name of the ad account.")
    currency: str | None = Field(default=None, description="The ISO 4217 currency code the ad account is billed in.")
    status: str | None = Field(default=None, description="The status of the ad account (e.g. ACTIVE, CANCELED, DRAFT).")
