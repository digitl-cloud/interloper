from interloper.schema import Schema
from pydantic import Field


class Audiences(Schema):
    """DV360 first-party and partner audience metadata."""

    first_party_and_partner_audience_id: str | None = Field(
        ..., description="The unique identifier for the audience"
    )
    display_name: str | None = Field(..., description="The display name of the audience")
    description: str | None = Field(..., description="The description of the audience")
    first_party_and_partner_audience_type: str | None = Field(
        ..., description="The type of first-party and partner audience"
    )
    audience_type: str | None = Field(..., description="The audience type")
    audience_source: str | None = Field(..., description="The source of the audience")
    membership_duration_days: int | None = Field(
        ..., description="The membership duration in days"
    )
    active_display_audience_size: int | None = Field(
        ..., description="The active display audience size"
    )
    app_id: str | None = Field(..., description="The app ID associated with the audience")
