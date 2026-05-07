from interloper.schema import Schema
from pydantic import Field


class AdCampaigns(Schema):
    """LinkedIn Ads campaign metadata including budget and scheduling details."""

    id: int = Field(description="The unique identifier of the ad campaign")
    name: str = Field(description="The name of the ad campaign")
    type: str = Field(description="The type of the ad campaign")
    status: str = Field(description="The status of the ad campaign")
    objective_type: str = Field(description="The objective type of the ad campaign")
    cost_type: str = Field(description="The type of cost associated with the ad campaign")
    campaign_group: str = Field(description="The campaign group associated with the ad campaign")
    run_schedule_start: int = Field(description="The start time of the ad campaign")
    run_schedule_end: int = Field(description="The end time of the ad campaign")
    daily_budget_currency_code: str = Field(description="The currency code of the daily budget for the ad campaign")
    daily_budget_amount: str = Field(description="The amount of the daily budget for the ad campaign")
