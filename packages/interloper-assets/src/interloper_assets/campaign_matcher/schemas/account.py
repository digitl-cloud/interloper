from interloper.schema import Schema
from pydantic import Field


class CampaignMatcher(Schema):
    campaign_id: str = Field(..., description="The ID of the campaign")
    campaign_name: str = Field(..., description="The name of the campaign")
    campaign_status: str = Field(..., description="The status of the campaign")
    campaign_start_date: str = Field(..., description="The start date of the campaign")
    campaign_end_date: str = Field(..., description="The end date of the campaign")
    campaign_budget: float = Field(..., description="The budget of the campaign")
    campaign_impressions: int = Field(..., description="The impressions of the campaign")
    campaign_clicks: int = Field(..., description="The clicks of the campaign")
    campaign_conversions: int = Field(..., description="The conversions of the campaign")
    campaign_cost: float = Field(..., description="The cost of the campaign")


class PerformanceAnalysis(Schema):
    campaign_id: str = Field(..., description="The ID of the campaign")
    campaign_name: str = Field(..., description="The name of the campaign")
    campaign_status: str = Field(..., description="The status of the campaign")
    campaign_start_date: str = Field(..., description="The start date of the campaign")
    campaign_end_date: str = Field(..., description="The end date of the campaign")
    campaign_budget: float = Field(..., description="The budget of the campaign")
    campaign_impressions: int = Field(..., description="The impressions of the campaign")
    campaign_clicks: int = Field(..., description="The clicks of the campaign")
    campaign_conversions: int = Field(..., description="The conversions of the campaign")
    campaign_cost: float = Field(..., description="The cost of the campaign")
