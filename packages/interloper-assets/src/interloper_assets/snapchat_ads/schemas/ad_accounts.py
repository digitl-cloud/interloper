import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """The Ad Accounts entity provides detailed information about each advertising account, including identifiers, timestamps, names, types, statuses, and various financial and organizational details."""

    id: str | None = Field(default=None, description="Unique identifier for the ad account.")
    updated_at: dt.datetime | None = Field(default=None, description="Timestamp of when the ad account was last updated.")
    created_at: dt.datetime | None = Field(default=None, description="Timestamp of when the ad account was created.")
    name: str | None = Field(default=None, description="Name of the ad account.")
    type: str | None = Field(default=None, description="Type of the ad account, e.g., PARTNER.")
    status: str | None = Field(default=None, description="Status of the ad account, e.g., ACTIVE.")
    organization_id: str | None = Field(default=None, description="Identifier for the organization associated with the ad account.")
    funding_source_ids: str | None = Field(default=None, description="List of funding source identifiers associated with the ad account.")
    currency: str | None = Field(default=None, description="Currency used by the ad account, e.g., USD.")
    timezone: str | None = Field(default=None, description="Timezone of the ad account, e.g., America/Los_Angeles.")
    advertiser: str | None = Field(default=None, description="Name of the advertiser associated with the ad account.")
    advertiser_organization_id: str | None = Field(default=None, description="Identifier for the advertiser organization associated with the ad account.")
    billing_center_id: str | None = Field(default=None, description="Identifier for the billing center associated with the ad account.")
    billing_type: str | None = Field(default=None, description="Type of billing for the ad account, e.g., IO.")
    lifetime_spend_cap_micro: int | None = Field(default=None, description="Lifetime spend cap for the ad account in micro-units of the currency.")
    agency_representing_client: bool | None = Field(default=None, description="Indicates whether an agency is representing the client.")
    client_paying_invoices: bool | None = Field(default=None, description="Indicates whether the client is paying the invoices.")
