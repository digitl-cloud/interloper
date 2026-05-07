from interloper.schema import Schema
from pydantic import Field


class CampaignsMetadata(Schema):
    """Metadata for campaigns including objective, status, budget, and delivery details."""

    id: str = Field(description="The ID of the campaign")
    updated_at: str = Field(description="Last updated timestamp of the campaign")
    created_at: str = Field(description="Creation timestamp of the campaign")
    name: str = Field(description="The name of the campaign")
    ad_account_id: str = Field(description="The ID of the associated ad account")
    status: str = Field(description="The status of the campaign")
    objective: str = Field(description="The objective of the campaign")
    start_time: str = Field(description="The start time of the campaign")
    end_time: str = Field(description="The end time of the campaign")
    lifetime_spend_cap_micro: float = Field(description="Lifetime spend cap in micro units")
    buy_model: str = Field(description="The buy model of the campaign")
    delivery_status: str = Field(description="The delivery status of the campaign")
    creation_state: str = Field(description="The creation state of the campaign")
    experiment_id: str = Field(description="The ID of the associated experiment")
