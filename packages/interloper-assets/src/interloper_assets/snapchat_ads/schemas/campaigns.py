from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """The Campaigns entity provides insights into the attributes of campaigns. It includes key dimensions such as campaign ID, name, objective, status, start and end times, lifetime spend cap, buy model, delivery status, and creation state."""

    id: str | None = Field(default=None, description="The ID of the campaign")
    updated_at: str | None = Field(default=None, description="The last updated timestamp of the campaign")
    created_at: str | None = Field(default=None, description="The creation timestamp of the campaign")
    name: str | None = Field(default=None, description="The name of the campaign")
    ad_account_id: str | None = Field(default=None, description="The ID of the associated ad account")
    status: str | None = Field(default=None, description="The status of the campaign")
    objective: str | None = Field(default=None, description="The objective of the campaign")
    start_time: str | None = Field(default=None, description="The start time of the campaign")
    end_time: str | None = Field(default=None, description="The end time of the campaign")
    lifetime_spend_cap_micro: float | None = Field(default=None, description="The lifetime spend cap of the campaign in micro units")
    buy_model: str | None = Field(default=None, description="The buy model of the campaign")
    delivery_status: str | None = Field(default=None, description="The delivery status of the campaign")
    creation_state: str | None = Field(default=None, description="The creation state of the campaign")
    experiment_id: str | None = Field(default=None, description="The ID of the associated experiment")
