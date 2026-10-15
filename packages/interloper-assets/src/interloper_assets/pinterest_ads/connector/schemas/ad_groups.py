import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdGroups(Schema):
    """Pinterest ad group snapshots with their budget, bidding, schedule and targeting settings."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_account_id: str | None = Field(default=None, description="Advertiser ID.")
    bid_in_micro_currency: float | None = Field(default=None, description="Bid price in micro currency.")
    bid_strategy_type: str | None = Field(default=None, description="Bid strategy type.")
    billable_event: str | None = Field(default=None, description="Ad group billable event type.")
    budget_in_micro_currency: float | None = Field(default=None, description="Budget in micro currency.")
    campaign_id: str | None = Field(default=None, description="Campaign ID of the ad group.")
    conversion_learning_mode_type: str | None = Field(default=None, description="oCPM learn mode")
    created_time: dt.datetime | None = Field(default=None, description="Ad group creation time.")
    customer_segment_id: str | None = Field(default=None, description="Customer segment ID applied to the ad group.")
    dca_assets: str | None = Field(default=None, description="[DCA] The Dynamic creative assets to use for DCA.")
    end_time: dt.datetime | None = Field(
        default=None, description="Timestamp in Unix format for scheduling when ads in the ad group stop appearing."
    )
    ext_features_enabled: str | None = Field(default=None, description="Tracking features.")
    feed_profile_id: str | None = Field(default=None, description="Feed Profile ID associated to the adgroup.")
    id: str | None = Field(default=None, description="Ad group ID.")
    is_creative_optimization: bool | None = Field(
        default=None, description="Enable creative optimization for the ad group, default value is FALSE."
    )
    is_local_inventory: bool | None = Field(
        default=None, description="Indicates whether the ad group should use the local inventory."
    )
    lifetime_frequency_cap: int | None = Field(
        default=None,
        description="Set a limit to the number of times a promoted pin from this campaign can be impressed by a pinner within the past rolling 30 days.",
    )
    local_inventory_radius_in_miles: float | None = Field(
        default=None, description="The targeting radius of the local inventory ads in miles."
    )
    name: str | None = Field(default=None, description="Ad group name.")
    optimization_goal_metadata_conversion_tag_v3_goal_metadata: str | None = Field(
        default=None,
        description="Optimization goals for objective-based performance campaigns (conversion_tag_v3_goal_metadata).",
    )
    optimization_goal_metadata_frequency_goal_metadata: str | None = Field(
        default=None, description="Frequency target can only be between 2 and 20"
    )
    optimization_goal_metadata_scrollup_goal_metadata: str | None = Field(
        default=None,
        description="Optimization goals for objective-based performance campaigns (scrollup_goal_metadata).",
    )
    performance_plus_campaign_settings_boost_prospecting_ad_group_bid: bool | None = Field(
        default=None, description="Whether to boost prospecting ad group bid."
    )
    performance_plus_campaign_settings_pinner_list_exclusions: str | None = Field(
        default=None, description="List of campaign-level exclusion pinner list IDs."
    )
    placement_group: str | None = Field(default=None, description="Placement group.")
    placement_traffic_type: str | None = Field(
        default=None,
        description="A targeting option that enables advertisers to choose whether to run ads in fullscreen feed, two column feed, or both",
    )
    promotion_application_level: str | None = Field(
        default=None, description="Specify if the promotion is applied at ad group or item level"
    )
    promotion_id: str | None = Field(default=None, description="Promotion ID.")
    promotion_ids: str | None = Field(default=None, description="Promotion IDs list.")
    start_time: dt.datetime | None = Field(
        default=None, description="Timestamp in Unix format for scheduling when ads in the ad group start to appear."
    )
    status: str | None = Field(default=None, description="Ad group/entity status.")
    summary_status: str | None = Field(default=None, description="Summary status for campaign")
    targeting_spec_age_bucket: str | None = Field(default=None, description="Predefined age ranges.")
    targeting_spec_apptype: str | None = Field(default=None, description="Allowed devices.")
    targeting_spec_audience_exclude: str | None = Field(default=None, description="Excluded customer list IDs.")
    targeting_spec_audience_include: str | None = Field(default=None, description="Targeted customer list IDs.")
    targeting_spec_gender: str | None = Field(default=None, description="Targeted genders.")
    targeting_spec_geo: str | None = Field(
        default=None, description="Region codes or postal codes to include for targeting."
    )
    targeting_spec_geo_exclude: str | None = Field(
        default=None, description="Region codes or postal codes to exclude from the targeting inclusion area."
    )
    targeting_spec_interest: str | None = Field(default=None, description="Array of interest object IDs.")
    targeting_spec_locale: str | None = Field(default=None, description="24 ISO 639-1 two-letter language codes.")
    targeting_spec_location: str | None = Field(
        default=None,
        description="Metropolitan codes and/or ISO-Alpha-2, two-letter country codes to include for targeting.",
    )
    targeting_spec_location_exclude: str | None = Field(
        default=None,
        description="Metropolitan codes and/or ISO-Alpha-2, two-letter country codes to exclude from targeting.",
    )
    targeting_spec_maximum_age: str | None = Field(default=None, description="Maximum age to target (inclusive).")
    targeting_spec_minimum_age: str | None = Field(default=None, description="Minimum age to target (inclusive).")
    targeting_spec_shopping_retargeting: str | None = Field(
        default=None,
        description="Array of object: lookback_window [Integer]: Number of days ago to start lookback timeframe for dynamic retargeting tag_types [Array of integer]: Event types to target for dynamic retargeting exclusion_window [Integer]: Number of days ago to stop lookback timeframe for dynamic retargeting",
    )
    targeting_spec_targeting_strategy: str | None = Field(
        default=None, description="The targeting strategies used for the ad"
    )
    targeting_template_ids: str | None = Field(
        default=None, description="Targeting template IDs applied to the ad group."
    )
    tracking_urls_audience_verification: str | None = Field(
        default=None, description="Third-party tracking URLs (audience_verification)."
    )
    tracking_urls_buyable_button: str | None = Field(
        default=None, description="Third-party tracking URLs (buyable_button)."
    )
    tracking_urls_click: str | None = Field(default=None, description="Third-party tracking URLs (click).")
    tracking_urls_engagement: str | None = Field(default=None, description="Third-party tracking URLs (engagement).")
    tracking_urls_impression: str | None = Field(default=None, description="Third-party tracking URLs (impression).")
    type: str | None = Field(default=None, description='Always "adgroup".')
    updated_time: dt.datetime | None = Field(default=None, description="Ad group last update time.")
    auto_targeting_enabled: bool | None = Field(default=None, description="Enable auto-targeting for ad group.")
    bid_multiplier: float | None = Field(default=None, description="Bid multiplier for ad group.")
    budget_type: str | None = Field(default=None, description="Budget type.")
    pacing_delivery_type: str | None = Field(default=None, description="Ad group pacing delivery type.")
