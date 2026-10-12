import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimCampaigns(Schema):
    """One row per Facebook Ads campaign, from its latest entity row, else its latest report, else the facts."""

    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    account_id: str | None = Field(default=None, description="Ad account the campaign belongs to.")
    objective: str | None = Field(default=None, description="Campaign objective.")
    status: str | None = Field(default=None, description="Configured status, known from the entity asset only.")
    date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
