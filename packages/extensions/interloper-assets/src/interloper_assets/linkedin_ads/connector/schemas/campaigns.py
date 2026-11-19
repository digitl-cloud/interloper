import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """LinkedIn campaign snapshot. One row per campaign and day, in every status: the whole campaign as LinkedIn returns it, with its type, objective, budgets, bidding, delivery settings, targeting, run schedule, serving statuses and audit stamps. Targeting criteria and offsite preferences are kept whole as JSON."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="The unique identifier of the campaign.")
    name: str | None = Field(default=None, description="The name of the campaign.")
    type: str | None = Field(default=None, description="The type of the campaign (e.g. SPONSORED_UPDATES, TEXT_AD).")
    status: str | None = Field(default=None, description="The status of the campaign (e.g. ACTIVE, PAUSED, ARCHIVED).")
    objective_type: str | None = Field(
        default=None, description="The objective of the campaign (e.g. WEBSITE_VISIT, LEAD_GENERATION)."
    )
    daily_budget_amount: float | None = Field(
        default=None, description="The daily budget of the campaign, in the budget currency."
    )
    daily_budget_currency_code: str | None = Field(
        default=None, description="The ISO 4217 currency code of the daily budget."
    )
    run_schedule_start: dt.datetime | None = Field(
        default=None, description="When the campaign's run schedule starts (UTC)."
    )
    run_schedule_end: dt.datetime | None = Field(
        default=None, description="When the campaign's run schedule ends (UTC); empty for an open-ended schedule."
    )
    cost_type: str | None = Field(default=None, description="How the campaign is billed (e.g. CPC, CPM, CPV).")
    campaign_group: str | None = Field(
        default=None, description="The URN of the campaign group the campaign belongs to."
    )
    account: str | None = Field(
        default=None, description="The URN of the ad account the campaign belongs to (urn:li:sponsoredAccount:...)."
    )
    associated_entity: str | None = Field(
        default=None, description="The URN of the organization or person the campaign advertises for."
    )
    format: str | None = Field(default=None, description="The ad format of the campaign (e.g. SINGLE_VIDEO, CAROUSEL).")
    creative_selection: str | None = Field(
        default=None, description="How the campaign rotates its creatives (OPTIMIZED or ROUND_ROBIN)."
    )
    optimization_target_type: str | None = Field(
        default=None, description="What the campaign's bidding optimizes for (e.g. MAX_REACH, MAX_CONVERSION, NONE)."
    )
    pacing_strategy: str | None = Field(
        default=None, description="How the campaign paces its budget (LIFETIME or ACCELERATED)."
    )
    political_intent: str | None = Field(
        default=None,
        description="Whether the campaign is declared political (POLITICAL, NOT_POLITICAL or NOT_DECLARED).",
    )
    total_budget_amount: float | None = Field(
        default=None, description="The total budget of the campaign, in the budget currency."
    )
    total_budget_currency_code: str | None = Field(
        default=None, description="The ISO 4217 currency code of the total budget."
    )
    unit_cost_amount: float | None = Field(
        default=None, description="The bid of the campaign per unit of its cost type, in the bid currency."
    )
    unit_cost_currency_code: str | None = Field(default=None, description="The ISO 4217 currency code of the bid.")
    locale_country: str | None = Field(
        default=None, description="The country of the campaign's locale (ISO 3166 code, e.g. DE)."
    )
    locale_language: str | None = Field(
        default=None, description="The language of the campaign's locale (ISO 639 code, e.g. de)."
    )
    audience_expansion_enabled: bool | None = Field(
        default=None, description="Whether LinkedIn may reach members similar to the targeted audience."
    )
    offsite_delivery_enabled: bool | None = Field(
        default=None, description="Whether the campaign also serves on the LinkedIn Audience Network."
    )
    story_delivery_enabled: bool | None = Field(
        default=None, description="Whether the campaign also serves as a story."
    )
    connected_television_only: bool | None = Field(
        default=None, description="Whether the campaign serves on connected TV only."
    )
    targeting_criteria_include_and: str | None = Field(
        default=None,
        description="JSON array of the audience the campaign targets: every entry must match, each an OR of facet values.",
    )
    targeting_criteria_exclude_or: str | None = Field(
        default=None, description="JSON object of the facet values the campaign excludes: any one excludes a member."
    )
    offsite_preferences_iab_categories_include: str | None = Field(
        default=None, description="JSON array of the IAB categories offsite delivery is limited to."
    )
    offsite_preferences_iab_categories_exclude: str | None = Field(
        default=None, description="JSON array of the IAB categories offsite delivery avoids."
    )
    offsite_preferences_publisher_restriction_files_include: str | None = Field(
        default=None, description="JSON array of the publisher allow lists of offsite delivery."
    )
    offsite_preferences_publisher_restriction_files_exclude: str | None = Field(
        default=None, description="JSON array of the publisher block lists of offsite delivery."
    )
    optimization_preference_frequency_optimization_preference_frequency: int | None = Field(
        default=None, description="How many impressions the frequency cap allows a member within its time span."
    )
    optimization_preference_frequency_optimization_preference_optimization_type: str | None = Field(
        default=None, description="The kind of frequency cap (e.g. MAX_FREQUENCY)."
    )
    optimization_preference_frequency_optimization_preference_time_span: str | None = Field(
        default=None, description="JSON object of the frequency cap's time span: its duration and unit (e.g. 7 DAY)."
    )
    serving_statuses: str | None = Field(
        default=None,
        description=(
            "JSON array of the campaign's serving statuses: RUNNABLE when it can serve, otherwise the reasons it cannot "
            "(e.g. STOPPED, BILLING_HOLD, CAMPAIGN_END_DATE_HOLD)."
        ),
    )
    test: bool | None = Field(default=None, description="Whether the campaign belongs to a test account.")
    version_version_tag: str | None = Field(
        default=None, description="The version tag of the campaign, which changes with every update."
    )
    change_audit_stamps_created_actor: str | None = Field(
        default=None, description="The URN of the member who created the campaign."
    )
    change_audit_stamps_created_time: dt.datetime | None = Field(
        default=None, description="When the campaign was created (UTC)."
    )
    change_audit_stamps_last_modified_actor: str | None = Field(
        default=None, description="The URN of the member who last modified the campaign."
    )
    change_audit_stamps_last_modified_time: dt.datetime | None = Field(
        default=None, description="When the campaign was last modified (UTC)."
    )
