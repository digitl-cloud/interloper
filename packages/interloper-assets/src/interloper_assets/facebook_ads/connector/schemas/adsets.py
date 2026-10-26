import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Adsets(Schema):
    """Facebook Ads ad set snapshot. One row per ad set and day: its schedule, budget, bidding, optimisation and targeting. id is the join key to adset_id in the insights tables. Budgets are numeric strings in the currency's minor units, times are ISO 8601 with the account's offset, as the Graph API returns them. Nested objects (targeting, promoted_object, bid_constraints...) are kept whole as JSON."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    account_id: str | None = Field(default=None, description="Ad account the ad set belongs to.")
    ad_set_goal: str | None = Field(default=None, description="JSON of the ad set's goal.")
    adlabels: str | None = Field(default=None, description="JSON array of ad labels attached to the ad set.")
    adset_schedule: str | None = Field(
        default=None, description="JSON array of day-parting windows; set when pacing_type is day_parting."
    )
    asset_feed_id: str | None = Field(default=None, description="Id of the asset feed for dynamic creative.")
    attribution_spec: str | None = Field(default=None, description="JSON array of the attribution windows.")
    bid_adjustments: str | None = Field(default=None, description="JSON of the bid adjustments.")
    bid_amount: int | None = Field(
        default=None, description="Bid cap or target, in the currency's minor units, when the bid strategy uses one."
    )
    bid_constraints: str | None = Field(default=None, description="JSON of the bid constraints, such as a ROAS floor.")
    bid_info: str | None = Field(default=None, description="JSON map of bid objective to amount.")
    bid_strategy: str | None = Field(
        default=None,
        description="LOWEST_COST_WITHOUT_CAP, LOWEST_COST_WITH_BID_CAP, COST_CAP or LOWEST_COST_WITH_MIN_ROAS.",
    )
    billing_event: str | None = Field(
        default=None, description="What the ad set is billed on: IMPRESSIONS, LINK_CLICKS..."
    )
    brand_safety_config: str | None = Field(default=None, description="JSON of the brand-safety configuration.")
    budget_remaining: str | None = Field(default=None, description="Remaining budget, in the currency's minor units.")
    campaign_active_time: str | None = Field(default=None, description="How long the campaign has been running.")
    campaign_attribution: str | None = Field(default=None, description="App attribution type (SKAN or AEM).")
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    configured_status: str | None = Field(
        default=None, description="Status set by the advertiser: ACTIVE, PAUSED, DELETED, ARCHIVED."
    )
    created_time: str | None = Field(default=None, description="When the ad set was created, ISO 8601 with offset.")
    creative_sequence: str | None = Field(default=None, description="JSON array of ad ids in sequence order.")
    daily_budget: str | None = Field(
        default=None, description="Daily budget, in the currency's minor units; NULL when the ad set has none."
    )
    daily_min_spend_target: str | None = Field(
        default=None, description="Daily minimum spend target when the campaign holds the budget."
    )
    daily_spend_cap: str | None = Field(default=None, description="Daily spend cap when the campaign holds the budget.")
    destination_type: str | None = Field(
        default=None, description="Where the ads send people: WEBSITE, APP, MESSENGER..."
    )
    dsa_beneficiary: str | None = Field(
        default=None, description="Beneficiary declared under the EU Digital Services Act."
    )
    dsa_payor: str | None = Field(default=None, description="Payor declared under the EU Digital Services Act.")
    effective_status: str | None = Field(
        default=None,
        description="Delivery status after Facebook's own checks: ACTIVE, PAUSED, CAMPAIGN_PAUSED, IN_PROCESS, WITH_ISSUES...",
    )
    end_time: str | None = Field(
        default=None, description="Scheduled end, ISO 8601 in the account's timezone; NULL when open-ended."
    )
    frequency_control_specs: str | None = Field(default=None, description="JSON array of frequency caps.")
    id: str | None = Field(default=None, description="Ad set id. Join key to adset_id in the insights tables.")
    instagram_user_id: str | None = Field(default=None, description="Instagram account the ad set delivers from.")
    is_dynamic_creative: bool | None = Field(default=None, description="Whether the ad set uses dynamic creative.")
    is_incremental_attribution_enabled: bool | None = Field(
        default=None, description="Whether incremental attribution is enabled."
    )
    issues_info: str | None = Field(default=None, description="JSON array of the ad set's delivery issues.")
    learning_stage_info: str | None = Field(default=None, description="JSON of the ad set's learning-phase status.")
    lifetime_budget: str | None = Field(
        default=None, description="Lifetime budget, in the currency's minor units; NULL when the ad set has none."
    )
    lifetime_imps: int | None = Field(default=None, description="Lifetime impressions, for FIXED_CPM buying only.")
    lifetime_min_spend_target: str | None = Field(
        default=None, description="Lifetime minimum spend target when the campaign holds the budget."
    )
    lifetime_spend_cap: str | None = Field(
        default=None, description="Lifetime spend cap when the campaign holds the budget."
    )
    min_budget_spend_percentage: str | None = Field(
        default=None, description="Minimum share of the campaign budget the ad set spends."
    )
    multi_optimization_goal_weight: str | None = Field(default=None, description="Weighting across optimisation goals.")
    name: str | None = Field(default=None, description="Ad set name.")
    optimization_goal: str | None = Field(
        default=None, description="What delivery optimises for: OFFSITE_CONVERSIONS, LINK_CLICKS, REACH..."
    )
    optimization_sub_event: str | None = Field(default=None, description="Sub-event of the optimisation goal.")
    pacing_type: str | None = Field(default=None, description="JSON array of pacing types: standard or day_parting.")
    promoted_object: str | None = Field(
        default=None, description="JSON of what the ad set promotes: pixel and event, app, page..."
    )
    recommendations: str | None = Field(default=None, description="JSON array of Meta's recommendations.")
    recurring_budget_semantics: bool | None = Field(
        default=None, description="Whether the budget recurs as a daily budget."
    )
    regional_regulated_categories: str | None = Field(
        default=None, description="JSON array of the regional regulated categories declared."
    )
    regional_regulation_identities: str | None = Field(
        default=None, description="JSON of the regional regulation identities declared."
    )
    review_feedback: str | None = Field(default=None, description="Review feedback for dynamic creative.")
    rf_prediction_id: str | None = Field(default=None, description="Reach and frequency prediction id.")
    source_adset_id: str | None = Field(default=None, description="Ad set this one was copied from.")
    start_time: str | None = Field(default=None, description="Scheduled start, ISO 8601 in the account's timezone.")
    status: str | None = Field(
        default=None, description="Status set by the advertiser: ACTIVE, PAUSED, DELETED, ARCHIVED."
    )
    targeting: str | None = Field(default=None, description="JSON of the full targeting specification.")
    targeting_optimization_types: str | None = Field(
        default=None, description="JSON of the targeting signals Meta may relax."
    )
    time_based_ad_rotation_id_blocks: str | None = Field(
        default=None, description="JSON array of the ad id blocks for time-based rotation."
    )
    time_based_ad_rotation_intervals: str | None = Field(
        default=None, description="JSON array of the time-based rotation intervals."
    )
    updated_time: str | None = Field(
        default=None, description="When the ad set was last changed, ISO 8601 with offset."
    )
    use_new_app_click: bool | None = Field(default=None, description="Whether the new app click definition is used.")
    value_rule_set_id: str | None = Field(default=None, description="Value rule set applied to the ad set.")
