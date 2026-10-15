from interloper.schema import Schema
from pydantic import Field


class CampaignsStatsByCity(Schema):
    """Campaign performance segmented by city with geographic coordinates and device demographics."""

    agent_id: int = Field(description="The ID of the agent")
    browser: str = Field(description="The browser used")
    camp_id: int = Field(description="The ID of the campaign")
    camp_title: str = Field(description="The title of the campaign")
    city: str = Field(description="The city")
    conversion_pct: int = Field(description="The conversion percentage")
    conversions: int = Field(description="The number of conversions")
    coordinates_lat: float = Field(description="The latitude coordinates")
    coordinates_lng: float = Field(description="The longitude coordinates")
    country: str = Field(description="The country")
    device_model: str = Field(description="The device model")
    device_type: str = Field(description="The device type")
    os: str = Field(description="The operating system")
