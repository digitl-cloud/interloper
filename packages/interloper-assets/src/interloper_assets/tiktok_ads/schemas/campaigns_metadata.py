from interloper.schema import Schema
from pydantic import Field


class CampaignsMetadata(Schema):
    """Metadata for TikTok campaigns including budget, objective, and status details."""

    campaign_id: str = Field(description="Unique identifier for the campaign")
    campaign_name: str = Field(description="Name of the campaign")
    advertiser_id: str = Field(description="Unique identifier for the advertiser")
    objective_type: str = Field(description="Type of campaign objective")
    objective: str = Field(description="Main objective of the campaign")
    campaign_type: str = Field(description="Type of campaign")
    budget: float = Field(description="Allocated budget for the campaign")
    budget_mode: str = Field(description="Mode of the campaign budget")
    budget_optimize_on: str = Field(description="How the budget is optimized")
    operation_status: str = Field(description="Operational status of the campaign")
    secondary_status: str = Field(description="Secondary status of the campaign")
    create_time: str = Field(description="Timestamp when the campaign was created")
    modify_time: str = Field(description="Last modification timestamp")
    app_promotion_type: str = Field(description="Type of app promotion")
    special_industries: str = Field(description="Special industries indicator")
    roas_bid: float = Field(description="ROAS bid for the campaign")
    rta_product_selection_enabled: bool = Field(description="Whether RTA product selection is enabled")
    is_new_structure: bool = Field(description="Whether the campaign follows new structure")
    is_smart_performance_campaign: bool = Field(description="Whether it is a smart performance campaign")
    is_search_campaign: bool = Field(description="Whether it is a search campaign")
    is_advanced_dedicated_campaign: bool = Field(description="Whether it is an advanced dedicated campaign")
    rf_campaign_type: str = Field(description="Type of Reach and Frequency campaign")
