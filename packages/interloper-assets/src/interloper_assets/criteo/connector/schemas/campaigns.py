import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """Criteo campaign snapshot. One row per campaign and day, as the campaign search returns it, nested objects flattened. A campaign carries no status or schedule: Criteo holds both on its ad sets."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Campaign id.")
    type: str | None = Field(default=None, description="Entity type Criteo names the resource with.")
    advertiser_id: str | None = Field(default=None, description="Advertiser the campaign belongs to.")
    name: str | None = Field(default=None, description="Campaign name.")
    goal: str | None = Field(default=None, description="Goal of the campaign: unspecified, acquisition or retention.")
    spend_limit_spend_limit_type: str | None = Field(
        default=None, description="Whether the campaign's spend is limited: capped or uncapped."
    )
    spend_limit_spend_limit_renewal: str | None = Field(
        default=None,
        description="Period the spend limit applies over: daily, monthly or lifetime; undefined when uncapped.",
    )
    spend_limit_spend_limit_amount_value: float | None = Field(
        default=None, description="Spend limit, in the advertiser's currency; empty when uncapped."
    )
    budget_automation_enabled: bool | None = Field(
        default=None, description="Whether Criteo allocates the budget across the campaign's ad sets."
    )
    budget_automation_automated_budget_configuration_ad_set_optimization_objective: str | None = Field(
        default=None,
        description="Objective the automated budget optimises for: conversions, revenue, visits or videoViews.",
    )
