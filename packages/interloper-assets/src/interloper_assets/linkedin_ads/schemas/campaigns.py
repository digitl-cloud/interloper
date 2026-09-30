import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """LinkedIn campaign snapshot. One row per campaign and day, with its type, objective, budget and run schedule."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: int | None = Field(default=None, description="The unique identifier of the campaign.")
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
