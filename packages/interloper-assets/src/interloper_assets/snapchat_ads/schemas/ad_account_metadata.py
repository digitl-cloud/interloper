from interloper.schema import Schema
from pydantic import Field


class AdAccountMetadata(Schema):
    """Metadata for a single ad account including billing and organizational details."""

    id: str = Field(description="Unique identifier for the ad account")
    updated_at: str = Field(description="Timestamp when the ad account was last updated")
    created_at: str = Field(description="Timestamp when the ad account was created")
    name: str = Field(description="Name of the ad account")
    type: str = Field(description="Type of the ad account")
    status: str = Field(description="Status of the ad account")
    organization_id: str = Field(description="Organization associated with the ad account")
    funding_source_ids: str = Field(description="Funding source identifiers")
    currency: str = Field(description="Currency used by the ad account")
    timezone: str = Field(description="Timezone of the ad account")
    advertiser: str = Field(description="Name of the advertiser")
    advertiser_organization_id: str = Field(description="Advertiser organization identifier")
    billing_center_id: str = Field(description="Billing center identifier")
    billing_type: str = Field(description="Type of billing for the ad account")
    lifetime_spend_cap_micro: int = Field(description="Lifetime spend cap in micro-units of the currency")
    agency_representing_client: bool = Field(description="Whether an agency is representing the client")
    client_paying_invoices: bool = Field(description="Whether the client is paying the invoices")
