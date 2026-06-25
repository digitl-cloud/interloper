import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """Tiktok campaign entities"""

    modify_time: dt.datetime | None = Field(default=None, description="Last modification timestamp of the campaign")
    special_industries: str | None = Field(default=None, description="Indicates if the campaign belongs to special industries")
    campaign_id: str | None = Field(default=None, description="Unique identifier for the campaign")
    budget: float | None = Field(default=None, description="Allocated budget for the campaign")
    advertiser_id: str | None = Field(default=None, description="Unique identifier for the advertiser")
    budget_mode: str | None = Field(default=None, description="Mode of the campaign budget (e.g., daily or total)")
    campaign_name: str | None = Field(default=None, description="Name of the campaign")
    roas_bid: float | None = Field(default=None, description="ROAS (Return on Ad Spend) bid for the campaign")
    objective_type: str | None = Field(default=None, description="Type of campaign objective (e.g., conversions, traffic)")
    rta_product_selection_enabled: bool | None = Field(default=None, description="Indicates if RTA product selection is enabled")
    is_new_structure: bool | None = Field(default=None, description="Indicates if the campaign follows the new structure")
    is_smart_performance_campaign: bool | None = Field(default=None, description="Indicates if it is a smart performance campaign")
    create_time: dt.datetime | None = Field(default=None, description="Timestamp when the campaign was created")
    is_search_campaign: bool | None = Field(default=None, description="Indicates if it is a search campaign")
    operation_status: str | None = Field(default=None, description="Operational status of the campaign")
    is_advanced_dedicated_campaign: bool | None = Field(default=None, description="Indicates if it is an advanced dedicated campaign")
    app_promotion_type: str | None = Field(default=None, description="Type of app promotion in the campaign")
    objective: str | None = Field(default=None, description="Main objective of the campaign")
    campaign_type: str | None = Field(default=None, description="Type of campaign (e.g., awareness, consideration)")
    secondary_status: str | None = Field(default=None, description="Secondary status of the campaign")
    budget_optimize_on: str | None = Field(default=None, description="Indicates how the budget is optimized")
    rf_campaign_type: str | None = Field(default=None, description="Type of Reach and Frequency campaign")
