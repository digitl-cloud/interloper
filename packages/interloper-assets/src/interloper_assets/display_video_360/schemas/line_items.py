from interloper.schema import Schema
from pydantic import Field


class LineItems(Schema):
    """DV360 line item metadata including budget, flight dates, and bidding strategy."""

    advertiser_id: str | None = Field(..., description="The advertiser ID")
    campaign_id: str | None = Field(..., description="The campaign ID")
    insertion_order_id: str | None = Field(..., description="The insertion order ID")
    line_item_id: str | None = Field(..., description="The line item ID")
    display_name: str | None = Field(..., description="The display name of the line item")
    entity_status: str | None = Field(..., description="The entity status")
    update_time: str | None = Field(..., description="The last update time")
    line_item_type: str | None = Field(..., description="The type of line item")
    flight_start_date: str | None = Field(..., description="The flight start date")
    flight_end_date: str | None = Field(..., description="The flight end date")
    budget_amount_micros: str | None = Field(..., description="The budget amount in micros")
    budget_unit: str | None = Field(..., description="The budget unit")
    pacing_period: str | None = Field(..., description="The pacing period")
    pacing_type: str | None = Field(..., description="The pacing type")
    bidding_strategy_type: str | None = Field(..., description="The bidding strategy type")
    bidding_strategy_max_average_cpm_bid_amount_micros: str | None = Field(
        ..., description="The max average CPM bid amount in micros"
    )
    bidding_strategy_performance_goal_type: str | None = Field(
        ..., description="The performance goal type for the bidding strategy"
    )
    bidding_strategy_performance_goal_amount_micros: str | None = Field(
        ..., description="The performance goal amount in micros"
    )
    creative_ids: str | None = Field(..., description="The creative IDs associated with the line item")
    partner_revenue_model_markup_type: str | None = Field(..., description="The partner revenue model markup type")
    partner_revenue_model_markup_amount: str | None = Field(
        ..., description="The partner revenue model markup amount"
    )
