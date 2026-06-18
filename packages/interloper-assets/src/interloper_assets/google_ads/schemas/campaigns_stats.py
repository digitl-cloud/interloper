import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStats(Schema):
    """Google Ads campaign performance report with metrics like impressions, clicks, conversions, and cost."""

    date: dt.date = Field(description="The date of the aggregated metrics")
    customer_id: str = Field(description="The Google Ads customer ID")
    customer_descriptive_name: str = Field(description="The Google Ads customer name")
    campaign_id: str = Field(description="The unique identifier for the campaign")
    campaign_name: str = Field(description="The name of the campaign")
    campaign_resource_name: str = Field(description="The full resource name of the campaign")
    campaign_status: str = Field(description="Campaign status (e.g., ENABLED, PAUSED)")
    campaign_advertising_channel_type: str = Field(description="Primary channel type (e.g., SEARCH, DISPLAY)")
    campaign_bidding_strategy_type: str = Field(description="The bidding strategy used for the campaign")
    campaign_start_date: str = Field(description="The campaign start date")
    campaign_end_date: str = Field(description="The campaign end date")
    campaign_budget_amount_micros: int = Field(description="The campaign budget amount in micros")
    clicks: int = Field(description="Total number of clicks")
    impressions: int = Field(description="Total number of impressions")
    interactions: int = Field(description="Total number of interactions")
    cost_micros: int = Field(description="Total cost in micro-units (1,000,000 micros = 1 currency unit)")
    conversions: float = Field(description="Total number of conversions (primary)")
    conversions_value: float = Field(description="Total value of conversions (primary)")
    all_conversions: float = Field(description="Total number of all conversions including cross-device")
    all_conversions_value: float = Field(description="Total value of all conversions")
    ctr: float = Field(description="Click-through rate (clicks / impressions)")
    interaction_rate: float = Field(description="Interaction rate (interactions / impressions)")
    conversions_from_interactions_rate: float = Field(description="Conversions per interaction")
    average_cpc: float = Field(description="Average cost-per-click")
    average_cpm: float = Field(description="Average cost-per-thousand impressions")
    average_cpv: float = Field(description="Average cost-per-view for video campaigns")
    average_cost: float = Field(description="Average cost per interaction")
    cost_per_all_conversions: float = Field(description="Cost per all conversions")
    search_impression_share: float = Field(description="Search Network impression share")
    search_absolute_top_impression_share: float = Field(description="Absolute top impression share on Search")
