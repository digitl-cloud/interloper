import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """Pinterest campaign snapshots with their objective, spend caps, schedule and status."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_account_id: str | None = Field(default=None, description="Campaign's Advertiser ID.")
    bid_options_age_bucket_multipliers: str | None = Field(
        default=None, description="Age bucket multipliers for bid adjustments."
    )
    bid_options_app_type_multipliers: str | None = Field(
        default=None, description="App type multipliers for bid adjustments."
    )
    bid_options_audience_multipliers: str | None = Field(
        default=None, description="Audience multipliers for bid adjustments."
    )
    bid_options_freq_bid_multiplier_time_window: str | None = Field(
        default=None, description="The time window for frequency bid multipliers."
    )
    bid_options_frequency_multipliers: str | None = Field(
        default=None, description="Frequency multipliers for bid adjustments."
    )
    bid_options_gender_multipliers: str | None = Field(
        default=None, description="Gender multipliers for bid adjustments."
    )
    bid_options_placement_multipliers: str | None = Field(
        default=None, description="Placement multipliers for bid adjustments."
    )
    created_time: dt.datetime | None = Field(default=None, description="Campaign creation time.")
    daily_spend_cap: float | None = Field(
        default=None, description="Campaign daily spend cap, in micro currency; null is treated as 0."
    )
    default_ad_group_budget_in_micro_currency: float | None = Field(
        default=None,
        description="Daily budget propagated to each child ad group when leaving campaign budget optimization, in micro currency.",
    )
    end_time: dt.datetime | None = Field(
        default=None, description="Timestamp in Unix format for scheduling when ads in the campaign stop appearing."
    )
    id: str | None = Field(
        default=None, description="Campaign ID, must be associated with the ad account ID provided in the path."
    )
    intended_promotion_type: str | None = Field(
        default=None, description="Specifies the intended promotion type for the campaign."
    )
    is_automated_campaign: bool | None = Field(
        default=None, description="Specifies whether the campaign was created in the automated campaign flow."
    )
    is_campaign_budget_optimization: bool | None = Field(
        default=None,
        description="Determines if a campaign automatically generates ad-group level budgets given a campaign budget to maximize campaign outcome.",
    )
    is_carting: bool | None = Field(
        default=None, description="Whether the campaign contains a carting(where-to-buy link) ad."
    )
    is_flexible_daily_budgets: bool | None = Field(
        default=None,
        description='Determine if a campaign has setup for flexible daily budgets, also known as "Pinterest Performance+ budgets".',
    )
    is_ltv_optimized: bool | None = Field(
        default=None, description="Specifies whether the campaign is optimized for Lifetime Value (LTV)."
    )
    is_performance_plus: bool | None = Field(
        default=None, description="Whether Pinterest Performance+ is enabled for the campaign."
    )
    is_top_of_search: bool | None = Field(
        default=None, description="Whether the campaign's ads appear at the top of search results."
    )
    lifetime_spend_cap: float | None = Field(
        default=None, description="Campaign lifetime spend cap, in micro currency; null is treated as 0."
    )
    name: str | None = Field(default=None, description="Campaign name - 255 chars max.")
    objective_type: str | None = Field(default=None, description="Campaign objective type.")
    order_line_id: str | None = Field(default=None, description="Order line ID that appears on the invoice.")
    performance_plus_campaign_settings_boost_prospecting_ad_group_bid: bool | None = Field(
        default=None, description="Whether to boost prospecting ad group bid."
    )
    performance_plus_campaign_settings_pinner_list_exclusions: str | None = Field(
        default=None, description="List of campaign-level exclusion pinner list IDs."
    )
    start_time: dt.datetime | None = Field(
        default=None, description="Timestamp in Unix format for scheduling when ads in the campaign start to appear."
    )
    status: str | None = Field(default=None, description="Entity status")
    summary_status: str | None = Field(default=None, description="Summary status for campaign")
    tracking_urls_audience_verification: str | None = Field(
        default=None, description="Third-party tracking URLs (audience_verification)."
    )
    tracking_urls_buyable_button: str | None = Field(
        default=None, description="Third-party tracking URLs (buyable_button)."
    )
    tracking_urls_click: str | None = Field(default=None, description="Third-party tracking URLs (click).")
    tracking_urls_engagement: str | None = Field(default=None, description="Third-party tracking URLs (engagement).")
    tracking_urls_impression: str | None = Field(default=None, description="Third-party tracking URLs (impression).")
    type: str | None = Field(default=None, description='Always "campaign".')
    updated_time: dt.datetime | None = Field(default=None, description="UTC timestamp.")
