import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DisplayCampaigns(Schema):
    """Sponsored Display campaign snapshot. One row per campaign and day, as the extended campaigns list returns it. Ids are numeric, as Sponsored Display sends them. Creation and update times are epoch numbers whose unit Amazon does not document, kept as sent."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    campaign_id: int | None = Field(default=None, description="Campaign id.")
    name: str | None = Field(default=None, description="Campaign name.")
    state: str | None = Field(default=None, description="enabled, paused or archived.")
    tactic: str | None = Field(default=None, description="Targeting tactic: T00020 (contextual) or T00030 (audiences).")
    budget: float | None = Field(default=None, description="Budget amount, in the profile's currency.")
    budget_type: str | None = Field(default=None, description="Budget type: daily.")
    start_date: dt.date | None = Field(default=None, description="Scheduled start, in the profile's timezone.")
    end_date: dt.date | None = Field(
        default=None, description="Scheduled end, in the profile's timezone; NULL when open-ended."
    )
    cost_type: str | None = Field(default=None, description="cpc or vcpm.")
    portfolio_id: int | None = Field(default=None, description="Portfolio the campaign belongs to.")
    rule_based_budget_applicable_rule_id: str | None = Field(default=None, description="Budget rule in effect.")
    rule_based_budget_applicable_rule_name: str | None = Field(
        default=None, description="Name of the budget rule in effect."
    )
    rule_based_budget_is_processing: bool | None = Field(
        default=None, description="Whether the budget rule is being applied."
    )
    rule_based_budget_value: float | None = Field(default=None, description="Budget set by the rule.")
    serving_status: str | None = Field(default=None, description="Serving status.")
    creation_date: int | None = Field(default=None, description="When the campaign was created, as an epoch number.")
    last_updated_date: int | None = Field(
        default=None, description="When the campaign was last changed, as an epoch number."
    )
