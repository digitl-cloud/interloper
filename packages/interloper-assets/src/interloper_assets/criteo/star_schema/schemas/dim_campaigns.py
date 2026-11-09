import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Advertiser the campaign belongs to.")
    campaign_name: str | None = Field(
        default=None, description="Campaign name, from the entity or the campaign report."
    )
    objective: str | None = Field(
        default=None, description="Criteo's campaign goal: unspecified, acquisition or retention; entity rows only."
    )
    budget: float | None = Field(
        default=None,
        description="Campaign spend limit, in the advertiser's currency; NULL when uncapped; entity rows only.",
    )
    budget_type: str | None = Field(
        default=None,
        description="Period the spend limit applies over: daily, monthly, lifetime, or undefined when uncapped; "
        "entity rows only.",
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
