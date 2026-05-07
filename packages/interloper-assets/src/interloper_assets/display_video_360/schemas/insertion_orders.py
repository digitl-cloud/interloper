from interloper.schema import Schema
from pydantic import Field


class InsertionOrders(Schema):
    """DV360 insertion order metadata including budget, pacing, and performance goals."""

    advertiser_id: str | None = Field(..., description="The advertiser ID")
    insertion_order_id: str | None = Field(..., description="The insertion order ID")
    display_name: str | None = Field(..., description="The display name of the insertion order")
    entity_status: str | None = Field(..., description="The entity status")
    update_time: str | None = Field(..., description="The last update time")
    insertion_order_type: str | None = Field(..., description="The type of insertion order")
    pacing_period: str | None = Field(..., description="The pacing period")
    pacing_type: str | None = Field(..., description="The pacing type")
    budget_type: str | None = Field(..., description="The budget type")
    budget_automation_type: str | None = Field(..., description="The budget automation type")
    budget_segments: str | None = Field(..., description="The budget segments (JSON)")
    performance_goal_type: str | None = Field(..., description="The performance goal type")
    performance_goal_string: str | None = Field(..., description="The performance goal string")
