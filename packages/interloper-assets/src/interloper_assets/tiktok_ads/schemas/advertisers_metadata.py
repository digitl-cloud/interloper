from interloper.schema import Schema
from pydantic import Field


class AdvertisersMetadata(Schema):
    """Metadata for TikTok advertisers including account and company details."""

    advertiser_id: str = Field(description="Unique identifier for the advertiser")
    name: str = Field(description="Name of the advertiser")
    company: str = Field(description="Company name of the advertiser")
    address: str = Field(description="Physical address of the advertiser")
    country: str = Field(description="Country of the advertiser")
    currency: str = Field(description="Currency used for transactions")
    balance: float = Field(description="Account balance of the advertiser")
    status: str = Field(description="Current status of the advertiser")
    role: str = Field(description="Role of the advertiser")
    description: str = Field(description="Description about the advertiser")
    industry: str = Field(description="Industry category of the advertiser")
    language: str = Field(description="Primary language of the advertiser")
    timezone: str = Field(description="Timezone used by the advertiser")
    display_timezone: str = Field(description="Timezone set for display")
    create_time: str = Field(description="Timestamp when the advertiser was created")
    license_no: str = Field(description="License number of the advertiser")
    license_url: str = Field(description="URL to the advertiser's license")
    owner_bc_id: str = Field(description="Business center ID of the owner")
    rejection_reason: str = Field(description="Reason for any rejection")
