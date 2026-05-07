from interloper.schema import Schema
from pydantic import Field


class AdSquadsMetadata(Schema):
    """Metadata for ad squads including targeting, bidding, and scheduling configuration."""

    id: str = Field(description="The ID of the ad squad")
    updated_at: str = Field(description="Last updated timestamp of the ad squad")
    created_at: str = Field(description="Creation timestamp of the ad squad")
    name: str = Field(description="The name of the ad squad")
    status: str = Field(description="The status of the ad squad")
    campaign_id: str = Field(description="The ID of the campaign")
    type: str = Field(description="The type of the ad squad")
    targeting_reach_status: str = Field(description="The reach status of the ad squad")
    placement: str = Field(description="The placement of the ad squad")
    billing_event: str = Field(description="The billing event of the ad squad")
    auto_bid: bool = Field(description="Whether auto bidding is enabled")
    target_bid: bool = Field(description="Whether target bidding is enabled")
    bid_strategy: str = Field(description="The bid strategy of the ad squad")
    lifetime_budget_micro: int = Field(description="The lifetime budget in micro units")
    start_time: str = Field(description="The start time of the ad squad")
    end_time: str = Field(description="The end time of the ad squad")
    optimization_goal: str = Field(description="The optimization goal")
    delivery_constraint: str = Field(description="The delivery constraint")
    pacing_type: str = Field(description="The pacing type")
    child_ad_type: str = Field(description="The child ad type")
    forced_view_setting: str = Field(description="The forced view setting")
    creation_state: str = Field(description="The creation state")
    delivery_status: str = Field(description="The delivery status")
    delivery_properties_version: float = Field(description="Version of the delivery properties")
    targeting_regulated_content: bool = Field(description="Whether targeting regulated content is enabled")
    targeting_demographics: str = Field(description="Targeting demographics")
    targeting_interests: str = Field(description="Targeting interests")
    targeting_geos: str = Field(description="Targeting geos")
    targeting_enable_targeting_expansion: bool = Field(description="Whether targeting expansion is enabled")
    targeting_devices: str = Field(description="Targeting devices")
    bid_micro: float = Field(description="Bid in micro units")
