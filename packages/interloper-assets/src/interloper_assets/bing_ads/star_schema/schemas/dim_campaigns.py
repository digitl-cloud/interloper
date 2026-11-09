import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Account the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    status: str | None = Field(
        default=None, description="Configured campaign status, as the snapshot, else the latest report, states it."
    )
    campaign_type: str | None = Field(
        default=None,
        description="Campaign type, as the snapshot names it (Search, Shopping, PerformanceMax...), else the report.",
    )
    budget: float | None = Field(
        default=None,
        description="Daily budget, in the account's currency; NULL on rows built from the report or the facts.",
    )
    budget_type: str | None = Field(
        default=None,
        description="Budget type (DailyBudgetStandard, DailyBudgetAccelerated); NULL on rows built from the report or the facts.",
    )
    start_date: dt.date | None = Field(
        default=None,
        description="Day the campaign starts serving, as Bing schedules it; NULL when unscheduled or on rows built "
        "from the report or the facts.",
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Last day the campaign serves, NULL when it runs on or on rows built from the report or the facts.",
    )
    updated_at: dt.datetime | None = Field(
        default=None, description="When the campaign last changed; NULL on rows built from the report or the facts."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
