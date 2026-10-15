from interloper.schema import Schema
from pydantic import Field


class CampaignsStatsByDeviceType(Schema):
    """Campaign performance segmented by device type with browser, OS, and geographic data."""

    agent_id: int = Field(description="The ID of the agent")
    browser: str = Field(description="The browser used")
    camp_id: int = Field(description="The ID of the campaign")
    camp_title: str = Field(description="The title of the campaign")
    city: str = Field(description="The city where the campaign was run")
    conversion_pct: int = Field(description="The percentage of conversions")
    conversions: int = Field(description="The number of conversions")
    country: str = Field(description="The country where the campaign was run")
    device_model: str = Field(description="The model of the device")
    device_type: str = Field(description="The type of the device")
    os: str = Field(description="The operating system used")
