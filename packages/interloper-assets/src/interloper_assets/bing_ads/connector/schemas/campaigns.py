import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """Bing Ads campaign snapshot. One row per campaign and day, as the Bulk API's Campaign record carries it."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    type: str | None = Field(default=None, description="Bulk record type, always Campaign.")
    status: str | None = Field(
        default=None, description="Configured status (Active, Paused, BudgetPaused, BudgetAndManualPaused, Deleted...)."
    )
    id: str | None = Field(default=None, description="Campaign id.")
    parent_id: str | None = Field(default=None, description="Account the campaign belongs to.")
    sub_type: str | None = Field(
        default=None, description="Campaign sub type, such as ShoppingSmart or BrandAwarenessVideoAds."
    )
    campaign: str | None = Field(default=None, description="Campaign name.")
    website: str | None = Field(default=None, description="Website of a Performance Max campaign.")
    client_id: str | None = Field(default=None, description="Free-text label the advertiser gives the record.")
    modified_time: dt.datetime | None = Field(default=None, description="When the campaign was last changed, in UTC.")
    multi_media_ad_bid_adjustment: int | None = Field(
        default=None, description="Bid adjustment for multimedia ads, in percent."
    )
    dynamic_description_enabled: bool | None = Field(
        default=None, description="Whether dynamic search ads may get dynamic descriptions."
    )
    time_zone: str | None = Field(default=None, description="Bing time zone the campaign is scheduled in.")
    budget: float | None = Field(default=None, description="Daily budget, in the account's currency.")
    budget_type: str | None = Field(
        default=None, description="Budget type (DailyBudgetStandard, DailyBudgetAccelerated)."
    )
    budget_name: str | None = Field(default=None, description="Name of the shared budget the campaign uses, if any.")
    budget_id: str | None = Field(default=None, description="Shared budget the campaign uses, if any.")
    destination_channel: str | None = Field(
        default=None, description="Destination channel of a Performance Max campaign."
    )
    is_multi_channel_campaign: bool | None = Field(
        default=None, description="Whether the campaign serves on several channels."
    )
    is_political: bool | None = Field(default=None, description="Whether the campaign carries political ads.")
    language: str | None = Field(default=None, description="Languages the campaign targets, semicolon separated.")
    url_expansion_opt_out: bool | None = Field(default=None, description="Whether final URL expansion is turned off.")
    bid_adjustment: int | None = Field(
        default=None, description="Bid adjustment for the Microsoft Audience Network, in percent."
    )
    disclaimer_ads_enabled: bool | None = Field(
        default=None, description="Whether the campaign's ads carry a disclaimer."
    )
    country_code: str | None = Field(default=None, description="Country of a Shopping campaign's products.")
    store_id: str | None = Field(default=None, description="Microsoft Merchant Center store of a Shopping campaign.")
    campaign_type: str | None = Field(
        default=None,
        description="Campaign type (Search, Shopping, Audience, DynamicSearchAds, PerformanceMax, Hotel...).",
    )
    priority: int | None = Field(
        default=None, description="Priority of a Shopping campaign among those sharing products."
    )
    local_inventory_ads_enabled: bool | None = Field(
        default=None, description="Whether a Shopping campaign shows local inventory ads."
    )
    experiment_id: str | None = Field(default=None, description="Experiment the campaign belongs to, if any.")
    tracking_template: str | None = Field(default=None, description="Tracking template.")
    custom_parameter: str | None = Field(default=None, description="Custom URL parameters, as Bulk serializes them.")
    source: str | None = Field(default=None, description="Domain or feed a dynamic search ads campaign serves from.")
    bid_strategy_id: str | None = Field(default=None, description="Portfolio bid strategy the campaign uses, if any.")
    bid_strategy_name: str | None = Field(default=None, description="Name of the portfolio bid strategy.")
    bid_strategy_type: str | None = Field(
        default=None, description="Bid strategy (EnhancedCpc, ManualCpc, MaxClicks, MaxConversions, TargetCpa...)."
    )
    bid_strategy_max_cpc: float | None = Field(
        default=None, description="Maximum cost per click of the bid strategy, in the account's currency."
    )
    bid_strategy_target_cpa: float | None = Field(
        default=None, description="Target cost per acquisition, in the account's currency."
    )
    bid_strategy_target_roas: float | None = Field(default=None, description="Target return on ad spend.")
    bid_strategy_target_ad_position: str | None = Field(
        default=None, description="Target ad position of a target impression share strategy."
    )
    bid_strategy_target_impression_share: float | None = Field(
        default=None, description="Target impression share, in percent."
    )
    bid_strategy_commission_rate: float | None = Field(
        default=None, description="Commission rate of a commission strategy, in percent."
    )
    bid_strategy_percent_max_cpc: float | None = Field(default=None, description="Maximum percent cost per click.")
    bid_strategy_target_cost_per_sale: float | None = Field(
        default=None, description="Target cost per sale, in percent."
    )
    bid_strategy_scope: str | None = Field(default=None, description="Scope of the bid strategy.")
    target_setting: str | None = Field(
        default=None, description="Criteria the campaign bids on only, semicolon separated."
    )
    ad_schedule_use_searcher_time_zone: bool | None = Field(
        default=None, description="Whether ad schedules follow the searcher's time zone."
    )
    domain_language: str | None = Field(default=None, description="Language of a dynamic search ads campaign's domain.")
    page_feed_ids: str | None = Field(default=None, description="Page feeds the campaign uses, semicolon separated.")
    feed_id: str | None = Field(default=None, description="Feed a Shopping or Hotel campaign uses.")
    final_url_suffix: str | None = Field(default=None, description="Final URL suffix.")
    auto_generated_text_assets_opt_out: bool | None = Field(
        default=None, description="Whether auto-generated text assets are turned off."
    )
    auto_generated_image_assets_opt_out: bool | None = Field(
        default=None, description="Whether auto-generated image assets are turned off."
    )
    cost_per_sale_opt_out: bool | None = Field(default=None, description="Whether cost per sale bidding is turned off.")
    should_serve_on_msan: bool | None = Field(
        default=None, description="Whether the campaign serves on the Microsoft Audience Network."
    )
    additional_conversion_value: float | None = Field(
        default=None, description="Value added to new customers' conversions."
    )
    new_customer_acquisition_goal_id: str | None = Field(
        default=None, description="New customer acquisition goal the campaign follows."
    )
    new_customer_acquisition_bid_only_mode: bool | None = Field(
        default=None, description="Whether the campaign bids only for new customers."
    )
    ai_search_enabled: bool | None = Field(default=None, description="Whether the campaign serves in AI search.")
    search_term_matching_opt_out: bool | None = Field(
        default=None, description="Whether search term matching is turned off."
    )
