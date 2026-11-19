import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignGroups(Schema):
    """LinkedIn campaign group snapshot. One row per campaign group and day, in every status: the whole campaign group as LinkedIn returns it, with its objective, budget, budget optimization, run schedule, serving statuses and audit stamps."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="The unique identifier of the campaign group.")
    name: str | None = Field(default=None, description="The name of the campaign group.")
    status: str | None = Field(
        default=None, description="The status of the campaign group (e.g. ACTIVE, PAUSED, ARCHIVED)."
    )
    run_schedule_start: dt.datetime | None = Field(
        default=None, description="When the campaign group's run schedule starts (UTC)."
    )
    run_schedule_end: dt.datetime | None = Field(
        default=None, description="When the campaign group's run schedule ends (UTC); empty for an open-ended schedule."
    )
    account: str | None = Field(
        default=None,
        description="The URN of the ad account the campaign group belongs to (urn:li:sponsoredAccount:...).",
    )
    objective_type: str | None = Field(
        default=None,
        description="The objective every campaign in the group shares (e.g. BRAND_AWARENESS, WEBSITE_VISIT); empty when unset.",
    )
    total_budget_amount: float | None = Field(
        default=None, description="The total budget of the campaign group, in the budget currency."
    )
    total_budget_currency_code: str | None = Field(
        default=None, description="The ISO 4217 currency code of the total budget."
    )
    budget_optimization_bid_strategy: str | None = Field(
        default=None,
        description="The bid strategy every campaign in the group uses (MAXIMUM_DELIVERY, MANUAL or COST_CAP).",
    )
    budget_optimization_budget_optimization_strategy: str | None = Field(
        default=None, description="How the group's budget is shared across its campaigns (DYNAMIC)."
    )
    serving_statuses: str | None = Field(
        default=None,
        description=(
            "JSON array of the campaign group's serving statuses: RUNNABLE when its campaigns can serve, otherwise the "
            "reasons they cannot (e.g. STOPPED, BILLING_HOLD, CAMPAIGN_GROUP_END_DATE_HOLD)."
        ),
    )
    allowed_campaign_types: str | None = Field(
        default=None,
        description="JSON array of the campaign types the group may hold (e.g. SPONSORED_UPDATES, TEXT_AD); ENTERPRISE accounts only.",
    )
    backfilled: bool | None = Field(
        default=None,
        description="Whether LinkedIn created the campaign group to hold existing campaigns (the default group).",
    )
    test: bool | None = Field(default=None, description="Whether the campaign group belongs to a test account.")
    change_audit_stamps_created_actor: str | None = Field(
        default=None, description="The URN of the member who created the campaign group."
    )
    change_audit_stamps_created_time: dt.datetime | None = Field(
        default=None, description="When the campaign group was created (UTC)."
    )
    change_audit_stamps_last_modified_actor: str | None = Field(
        default=None, description="The URN of the member who last modified the campaign group."
    )
    change_audit_stamps_last_modified_time: dt.datetime | None = Field(
        default=None, description="When the campaign group was last modified (UTC)."
    )
