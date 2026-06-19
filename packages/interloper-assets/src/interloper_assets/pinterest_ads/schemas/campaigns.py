from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """Pinterest campaign metadata including budget and scheduling details."""

    id: int = Field(description="Unique identifier for the campaign")
    ad_account_id: int = Field(description="Unique identifier for the ad account")
    name: str = Field(description="Name of the campaign")
    status: str = Field(description="Current status of the campaign")
    type: str = Field(description="Type of the campaign")
    summary_status: str = Field(description="Summary status of the campaign")
    objective_type: str = Field(description="Objective type of the campaign")
    order_line_id: int = Field(description="Unique identifier for the order line")
    daily_spend_cap: int = Field(description="Daily spending limit for the campaign")
    lifetime_spend_cap: int = Field(description="Lifetime spending limit for the campaign")
    is_campaign_budget_optimization: bool = Field(description="Indicates if the campaign uses budget optimization")
    is_flexible_daily_budgets: bool = Field(description="Indicates if the campaign has flexible daily budgets")
    start_time: int = Field(description="Timestamp when the campaign is scheduled to start")
    end_time: int = Field(description="Timestamp when the campaign is scheduled to end")
    created_time: int = Field(description="Timestamp when the campaign was created")
    updated_time: int = Field(description="Timestamp when the campaign was last updated")
