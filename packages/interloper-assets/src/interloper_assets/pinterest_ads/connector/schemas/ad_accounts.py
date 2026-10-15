import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """Pinterest ad account snapshot with its owner, country, currency and permissions."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    country: str | None = Field(default=None, description="Country ID from ISO 3166-1 alpha-2.")
    created_time: dt.datetime | None = Field(default=None, description="Creation time.")
    currency: str | None = Field(default=None, description="Currency Codes from ISO 4217")
    id: str | None = Field(default=None, description="The unique identifier for the Pinterest Ads account")
    name: str | None = Field(default=None, description="Ad account name.")
    owner_id: str | None = Field(default=None, description="The owning account's user ID.")
    owner_username: str | None = Field(default=None, description="Public username for the user account")
    permissions: str | None = Field(
        default=None, description="The permissions the connected user holds on the ad account."
    )
    time_zone: str | None = Field(
        default=None, description='The time zone of the ad account, in IANA format (e.g., "America/Los_Angeles").'
    )
    updated_time: dt.datetime | None = Field(
        default=None, description="The timestamp when the Pinterest Ads account was last updated"
    )
