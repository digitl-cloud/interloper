import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """Facebook Ads campaign entity snapshot. One row per campaign (not time-series). Captures campaign-level configuration, objective, budget, and status. boosted_object_id is the join key to facebook_insights.posts for Boost Post campaigns."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Unique campaign identifier. Join key to campaign insights table (campaign_id).")
    account_id: str | None = Field(default=None, description="Ad account ID this campaign belongs to.")
    adlabels: str | None = Field(default=None, description="JSON array of ad labels attached to this campaign.")
    bid_strategy: str | None = Field(default=None, description="Campaign-level bid strategy: LOWEST_COST_WITHOUT_CAP, LOWEST_COST_WITH_BID_CAP, COST_CAP, LOWEST_COST_WITH_MIN_ROAS. NULL when bid strategy is set at ad set level.")
    boosted_object_id: str | None = Field(default=None, description="Page post ID this campaign was created to boost, in {page_id}_{post_id} format. Populated only for campaigns created via the Boost Post button. NULL for campaigns created via Ads Manager. Alternative join key to facebook_insights.posts for Boost Post campaigns.")
    brand_lift_studies: str | None = Field(default=None, description="JSON array of brand lift study objects associated with this campaign.")
    budget_rebalance_flag: bool | None = Field(default=None, description="Whether Campaign Budget Optimisation (CBO) is enabled \u2014 budget is distributed across ad sets automatically.")
    budget_remaining: str | None = Field(default=None, description="Remaining budget in account currency cents. NULL if campaign uses lifetime budget or has no cap.")
    buying_type: str | None = Field(default=None, description="Ad buying type: AUCTION (default), FIXED_CPM (reserved).")
    can_create_brand_lift_study: bool | None = Field(default=None, description="Whether a brand lift study can be created for this campaign.")
    can_use_spend_cap: bool | None = Field(default=None, description="Whether a spend cap can be set on this campaign.")
    configured_status: str | None = Field(default=None, description="Status set by the advertiser: ACTIVE, PAUSED, DELETED, ARCHIVED.")
    created_time: str | None = Field(default=None, description="ISO 8601 timestamp when the campaign was created.")
    daily_budget: str | None = Field(default=None, description="Daily budget in account currency cents (returned as string by the API). NULL if campaign uses a lifetime budget.")
    effective_status: str | None = Field(default=None, description="Effective delivery status: ACTIVE, PAUSED, DELETED, PENDING_REVIEW, DISAPPROVED, PREAPPROVED, PENDING_BILLING_INFO, ARCHIVED, INACTIVE.")
    is_skadnetwork_attribution: bool | None = Field(default=None, description="Whether SKAdNetwork attribution is enabled (iOS 14+ privacy-safe attribution).")
    issues_info: str | None = Field(default=None, description="JSON array of delivery issue objects. NULL if no issues.")
    last_budget_toggling_time: str | None = Field(default=None, description="ISO 8601 timestamp of the last time the campaign budget was toggled on/off.")
    lifetime_budget: str | None = Field(default=None, description="Total lifetime budget in account currency cents. NULL if campaign uses a daily budget.")
    name: str | None = Field(default=None, description="Campaign name as set in Ads Manager.")
    objective: str | None = Field(default=None, description="Campaign objective: OUTCOME_AWARENESS, OUTCOME_ENGAGEMENT, OUTCOME_LEADS, OUTCOME_SALES, OUTCOME_TRAFFIC, OUTCOME_APP_PROMOTION.")
    pacing_type: str | None = Field(default=None, description="JSON array containing the pacing type: ['standard'] or ['day_parting'].")
    promoted_object: str | None = Field(default=None, description="JSON object describing what the campaign promotes (e.g. {page_id}, {pixel_id, custom_event_type}, {application_id}). Structure varies by objective.")
    recommendations: str | None = Field(default=None, description="JSON array of Facebook optimization recommendations for this campaign. NULL if none.")
    smart_promotion_type: str | None = Field(default=None, description="Smart campaign type (e.g. SMART_APP_PROMOTION). NULL for standard campaigns.")
    source_campaign_id: str | None = Field(default=None, description="If this campaign was copied, the ID of the original source campaign. NULL otherwise.")
    special_ad_categories: str | None = Field(default=None, description="JSON array of special ad category declarations: CREDIT, EMPLOYMENT, HOUSING, ISSUES_ELECTIONS_POLITICS, ONLINE_GAMBLING_AND_GAMING. Empty array for standard campaigns.")
    special_ad_category: str | None = Field(default=None, description="Primary special ad category (legacy single-value field). NONE for standard campaigns.")
    special_ad_category_country: str | None = Field(default=None, description="JSON array of country codes where the special ad category applies.")
    spend_cap: str | None = Field(default=None, description="Maximum total spend cap in account currency cents. NULL if no cap is set.")
    start_time: str | None = Field(default=None, description="ISO 8601 scheduled start time. NULL if the campaign starts immediately.")
    status: str | None = Field(default=None, description="Campaign status as set by the advertiser.")
    stop_time: str | None = Field(default=None, description="ISO 8601 scheduled stop time. NULL if the campaign has no end date.")
    topline_id: str | None = Field(default=None, description="Topline/IO number for managed accounts. NULL for self-serve accounts.")
    updated_time: str | None = Field(default=None, description="ISO 8601 timestamp when the campaign was last updated.")

