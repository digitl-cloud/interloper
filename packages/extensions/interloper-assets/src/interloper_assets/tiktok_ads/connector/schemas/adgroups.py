import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Adgroups(Schema):
    """TikTok ad group snapshot. One row per ad group and day, as the ad group endpoint returns it."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    adgroup_id: str | None = Field(default=None, description="The unique identifier of the ad group.")
    campaign_id: str | None = Field(default=None, description="The unique identifier of the campaign.")
    advertiser_id: str | None = Field(default=None, description="The unique identifier of the advertiser.")
    adgroup_name: str | None = Field(default=None, description="The name of the ad group.")
    placement_type: str | None = Field(default=None, description="The type of ad placement.")
    placements: str | None = Field(
        default=None, description="JSON array: information about the ad placements targeted."
    )
    inventory_filter_enabled: bool | None = Field(
        default=None, description="Flag indicating if inventory filter is enabled."
    )
    comment_disabled: bool | None = Field(default=None, description="Flag indicating if comments are disabled.")
    app_id: str | None = Field(default=None, description="The unique identifier of the app.")
    promotion_type: str | None = Field(default=None, description="The type of promotion.")
    app_download_url: str | None = Field(default=None, description="The URL for downloading the associated app.")
    package: str | None = Field(default=None, description="The package used for the ad group.")
    pixel_id: str | None = Field(default=None, description="The ID of the pixel used for tracking.")
    optimization_event: str | None = Field(default=None, description="The event used for optimization.")
    secondary_optimization_event: str | None = Field(
        default=None, description="Additional event used for optimization."
    )
    creative_material_mode: str | None = Field(default=None, description="The mode for creative materials.")
    modify_time: dt.datetime | None = Field(
        default=None, description="The timestamp for when the ad group was last modified."
    )
    create_time: dt.datetime | None = Field(
        default=None, description="The timestamp for when the ad group was created."
    )
    audience_ids: str | None = Field(default=None, description="JSON array: the IDs of the targeted audience.")
    excluded_audience_ids: str | None = Field(default=None, description="JSON array: the IDs of excluded audiences.")
    audience_type: str | None = Field(default=None, description="The type of audience being targeted.")
    location_ids: str | None = Field(default=None, description="JSON array: the IDs of targeted locations.")
    is_hfss: bool | None = Field(
        default=None, description="Flag indicating if high-frequency short sequences are included."
    )
    interest_category_ids: str | None = Field(
        default=None, description="JSON array: the IDs of interest categories for targeting."
    )
    interest_keyword_ids: str | None = Field(
        default=None, description="JSON array: the IDs of interest keywords for targeting."
    )
    age_groups: str | None = Field(default=None, description="JSON array: the targeted age groups for the ad group.")
    gender: str | None = Field(default=None, description="The targeted gender for the ad group.")
    languages: str | None = Field(default=None, description="JSON array: the targeted languages for the ad group.")
    operating_systems: str | None = Field(default=None, description="JSON array: the targeted operating systems.")
    network_types: str | None = Field(default=None, description="JSON array: the types of networks targeted.")
    device_price_ranges: str | None = Field(default=None, description="JSON array: the price ranges for devices.")
    min_android_version: str | None = Field(default=None, description="The minimum required Android version.")
    ios14_targeting: str | None = Field(default=None, description="Information about iOS 14 targeting settings.")
    device_model_ids: str | None = Field(default=None, description="JSON array: the IDs of targeted device models.")
    min_ios_version: str | None = Field(default=None, description="The minimum required iOS version.")
    budget_mode: str | None = Field(default=None, description="The mode for managing the budget.")
    budget: float | None = Field(default=None, description="The allocated budget for the ad group.")
    schedule_type: str | None = Field(default=None, description="The type of scheduling.")
    schedule_start_time: dt.datetime | None = Field(default=None, description="Scheduled start of delivery, in UTC.")
    schedule_end_time: dt.datetime | None = Field(
        default=None, description="Scheduled end of delivery, in UTC; open-ended when the schedule type is continuous."
    )
    dayparting: str | None = Field(default=None, description="Information about dayparting settings.")
    optimization_goal: str | None = Field(default=None, description="The goal set for optimization.")
    cpv_video_duration: str | None = Field(default=None, description="The duration for cost-per-view video.")
    conversion_window: str | None = Field(default=None, description="The window for tracking conversions.")
    pacing: str | None = Field(default=None, description="Information about the pacing settings.")
    billing_event: str | None = Field(default=None, description="The event used for billing.")
    skip_learning_phase: int | None = Field(
        default=None, description="Flag indicating if the learning phase is skipped."
    )
    bid_type: str | None = Field(default=None, description="The type of bidding.")
    bid_price: float | None = Field(default=None, description="The price set for bidding.")
    conversion_bid_price: float | None = Field(default=None, description="The bid price for conversions.")
    deep_bid_type: str | None = Field(default=None, description="The type of deep bid strategy.")
    deep_cpa_bid: float | None = Field(default=None, description="The bid amount for deep cost-per-action.")
    secondary_status: str | None = Field(default=None, description="The secondary status of the ad group.")
    operation_status: str | None = Field(default=None, description="The status of the operation.")
    frequency: int | None = Field(default=None, description="The frequency of ad display.")
    frequency_schedule: int | None = Field(default=None, description="The schedule for frequency capping.")
    statistic_type: str | None = Field(default=None, description="The type of statistics being tracked.")
    carrier_ids: str | None = Field(default=None, description="JSON array: the IDs of the targeted carriers.")
    carriers: str | None = Field(default=None, description="JSON array: information about the targeted carriers.")
    video_download_disabled: bool | None = Field(
        default=None, description="Flag indicating if video downloads are disabled."
    )
    blocked_pangle_app_ids: str | None = Field(
        default=None, description="JSON array: the IDs of the blocked Pangle apps."
    )
    action_category_ids: str | None = Field(
        default=None, description="JSON array: the IDs of the action categories associated with the ad group."
    )
    action_days: int | None = Field(default=None, description="The number of days the action has been performed.")
    video_actions: str | None = Field(default=None, description="JSON array: information about video-specific actions.")
    rf_purchased_type: str | None = Field(default=None, description="Type of purchased results.")
    purchased_impression: float | None = Field(default=None, description="Information about purchased impressions.")
    purchased_reach: float | None = Field(default=None, description="Information about purchased reach.")
    rf_estimated_cpr: float | None = Field(default=None, description="Estimated cost per result.")
    rf_estimated_frequency: float | None = Field(default=None, description="Estimated frequency of results.")
    included_pangle_audience_package_ids: str | None = Field(
        default=None, description="JSON array: the IDs of included Pangle audience packages."
    )
    excluded_pangle_audience_package_ids: str | None = Field(
        default=None, description="JSON array: the IDs of excluded Pangle audience packages."
    )
    is_new_structure: bool | None = Field(
        default=None, description="Flag indicating if the ad group follows a new structure."
    )
    is_smart_performance_campaign: bool | None = Field(
        default=None, description="Flag indicating if the campaign is using smart performance."
    )
    catalog_id: str | None = Field(default=None, description="The unique identifier of the catalog.")
    product_set_id: str | None = Field(default=None, description="The ID of the product set.")
    catalog_authorized_bc_id: str | None = Field(
        default=None, description="The authorized Business Center ID for the catalog."
    )
    audience_rule: str | None = Field(default=None, description="JSON object: the rule set for targeting the audience.")
    included_custom_actions: str | None = Field(
        default=None, description="JSON array: custom actions that are included."
    )
    excluded_custom_actions: str | None = Field(
        default=None, description="JSON array: custom actions that are excluded."
    )
    shopping_ads_retargeting_type: str | None = Field(
        default=None, description="The type of retargeting used for shopping ads."
    )
    split_test_adgroup_ids: str | None = Field(
        default=None, description="JSON array: the IDs of ad groups participating in split testing."
    )
    brand_safety_type: str | None = Field(default=None, description="The type of brand safety measures.")
    brand_safety_partner: str | None = Field(default=None, description="Information about the brand safety partners.")
    promotion_website_type: str | None = Field(default=None, description="The type of website used for promotion.")
    ios_quota_type: str | None = Field(default=None, description="The type of iOS quota.")
    roas_bid: float | None = Field(default=None, description="The bid amount set for return on ad spend.")
    actions: str | None = Field(
        default=None, description="JSON array: information about the actions taken on the ad group."
    )
    targeting_expansion: str | None = Field(default=None, description="JSON object: settings for targeting expansion.")
    schedule_infos: str | None = Field(
        default=None, description="JSON array: information about the scheduling arrangements."
    )
    share_disabled: bool | None = Field(default=None, description="Flag indicating if sharing is disabled.")
    auto_targeting_enabled: bool | None = Field(
        default=None, description="Flag indicating if auto-targeting is enabled."
    )
    ios14_quota_type: str | None = Field(default=None, description="The type of iOS 14 quota.")
    campaign_name: str | None = Field(default=None, description="The name of the campaign.")
    bid_display_mode: str | None = Field(default=None, description="The display mode for bidding.")
    scheduled_budget: float | None = Field(default=None, description="The budget allocated for scheduling.")
    adgroup_app_profile_page_state: str | None = Field(
        default=None, description="The state of the app profile page related to the ad group."
    )
    keywords: str | None = Field(default=None, description="Keywords associated with the ad group.")
    next_day_retention: float | None = Field(default=None, description="Retention information for the next day.")
    category_id: str | None = Field(default=None, description="The ID of the category for the ad group.")
    search_result_enabled: bool | None = Field(
        default=None, description="Flag indicating if search results are enabled."
    )
    app_type: str | None = Field(default=None, description="The type of the associated app.")
    feed_type: str | None = Field(default=None, description="The type of feed used.")
    delivery_mode: str | None = Field(default=None, description="The mode for delivery.")
    category_exclusion_ids: str | None = Field(
        default=None, description="JSON array: the IDs of the excluded categories."
    )
    contextual_tag_ids: str | None = Field(
        default=None, description="JSON array: the IDs of contextual tags for targeting."
    )
    household_income: str | None = Field(default=None, description="JSON array: the targeted household income groups.")
    isp_ids: str | None = Field(
        default=None, description="JSON array: the IDs of the targeted internet service providers."
    )
    spending_power: str | None = Field(default=None, description="Information about the spending power targeted.")
    zipcode_ids: str | None = Field(default=None, description="JSON array: the IDs of targeted ZIP codes.")
