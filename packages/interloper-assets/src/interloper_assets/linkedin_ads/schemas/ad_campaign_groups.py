from interloper.schema import Schema
from pydantic import Field


class AdCampaignGroups(Schema):
    """LinkedIn Ads campaign group metadata including scheduling information."""

    id: int = Field(description="The unique identifier of the ad campaign group")
    name: str = Field(description="The name of the ad campaign group")
    status: str = Field(description="The status of the ad campaign group")
    run_schedule_start: int = Field(description="The start time of the ad campaign group's run schedule")
