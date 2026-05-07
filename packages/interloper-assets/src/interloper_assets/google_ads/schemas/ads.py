import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsReport(Schema):
    """Google Ads ad group ad performance report with metrics segmented by device, click type, and keyword."""

    date: dt.date = Field(description="The date of the aggregated metrics")
    customer_id: str = Field(description="The Google Ads customer ID")
    customer_descriptive_name: str = Field(description="The Google Ads customer name")
    customer_resource_name: str = Field(description="The full resource name for the customer")
    campaign_id: str = Field(description="The Google Ads campaign ID")
    campaign_name: str = Field(description="The campaign name")
    campaign_resource_name: str = Field(description="The full resource name for the campaign")
    campaign_status: str = Field(description="Campaign status (e.g., ENABLED, PAUSED)")
    campaign_advertising_channel_type: str = Field(description="Primary channel type (e.g., SEARCH, DISPLAY)")
    campaign_bidding_strategy_type: str = Field(description="The bidding strategy used")
    campaign_start_date: str = Field(description="The campaign start date")
    campaign_end_date: str = Field(description="The campaign end date")
    campaign_network_settings_target_search_network: bool = Field(description="True if targeting Google Search")
    campaign_network_settings_target_partner_search_network: bool = Field(
        description="True if targeting Search Partners"
    )
    ad_group_id: int = Field(description="The Google Ads ad group ID")
    ad_group_name: str = Field(description="The ad group name")
    ad_group_resource_name: str = Field(description="The full resource name for the ad group")
    ad_group_ad_id: str = Field(description="The Google Ads ad ID")
    ad_group_ad_name: str = Field(description="The ad name")
    ad_group_ad_type: str = Field(description="The ad format (e.g., RESPONSIVE_SEARCH_AD)")
    ad_group_ad_resource_name: str = Field(description="The full resource name for the ad group ad")
    ad_group_ad_final_urls: str = Field(description="The array of final landing page URLs")
    device: str = Field(description="The device segment (e.g., MOBILE, DESKTOP)")
    click_type: str = Field(description="The component clicked (e.g., HEADLINE, SITELINK)")
    keyword_info_text: str = Field(description="The keyword text that generated the impression/click")
    keyword_info_match_type: str = Field(description="The match type used (e.g., EXACT, PHRASE)")
    keyword_ad_group_criterion: str = Field(description="The resource name of the keyword criterion")
    clicks: int = Field(description="Total number of clicks")
    impressions: int = Field(description="Total number of impressions")
    interactions: int = Field(description="Total number of interactions")
    cost_micros: int = Field(description="Total cost in micro-units (1,000,000 micros = 1 currency unit)")
    conversions: float = Field(description="Conversions (primary)")
    conversions_value: float = Field(description="Value of conversions (primary)")
    all_conversions: float = Field(description="All conversions (all types)")
    all_conversions_value: float = Field(description="Value of all conversions")
    ctr: float = Field(description="Click-through rate (clicks / impressions)")
    interaction_rate: float = Field(description="Interaction rate (interactions / impressions)")
    conversions_from_interactions_rate: float = Field(description="Conversions per interaction")
    average_cpc: float = Field(description="Average cost-per-click")
    average_cpm: float = Field(description="Average cost-per-thousand impressions")
    average_cost: float = Field(description="Average cost per interaction")
    cost_per_all_conversions: float = Field(description="Cost per all conversions")
