from interloper.schema import Schema
from pydantic import Field


class Advertisers(Schema):
    """Tiktok Metadata for advertisers"""

    address: str | None = Field(default=None, description="Physical address of the advertiser")
    advertiser_id: str | None = Field(default=None, description="Unique identifier for the advertiser")
    balance: float | None = Field(default=None, description="Account balance of the advertiser")
    company: str | None = Field(default=None, description="Company name of the advertiser")
    country: str | None = Field(default=None, description="Country of the advertiser")
    create_time: str | None = Field(default=None, description="Timestamp when the advertiser was created")
    currency: str | None = Field(default=None, description="Currency used for the advertiser's transactions")
    description: str | None = Field(default=None, description="Description or notes about the advertiser")
    display_timezone: str | None = Field(default=None, description="Timezone set for the advertiser's display")
    industry: str | None = Field(default=None, description="Industry category of the advertiser")
    language: str | None = Field(default=None, description="Primary language of the advertiser")
    license_no: str | None = Field(default=None, description="License number of the advertiser")
    license_url: str | None = Field(default=None, description="URL to the advertiser's license")
    name: str | None = Field(default=None, description="Name of the advertiser")
    owner_bc_id: str | None = Field(default=None, description="Business center ID of the advertiser's owner")
    rejection_reason: str | None = Field(default=None, description="Reason for any rejection of the advertiser's application")
    role: str | None = Field(default=None, description="Role of the advertiser in the system")
    status: str | None = Field(default=None, description="Current status of the advertiser")
    timezone: str | None = Field(default=None, description="Timezone used by the advertiser")
