import datetime as dt

import interloper as il
from pydantic import Field


class CampaignMatches(il.Schema):
    date: dt.date | None = Field(default=None, description="Snapshot day, stamped from the run's partition.")
    platform: str = Field(description="The advertising platform the campaign lives on, as the connector's catalog key.")
    account: str = Field(description="The advertising account the campaign belongs to, as configured on the connector.")
    campaign_id: str = Field(description="The campaign's id on its platform.")
    campaign_name: str = Field(description="The campaign's name as its platform reports it.")
    match_id: str = Field(description="The id shared by every campaign matched to the same canonical campaign.")
    canonical_name: str = Field(description="The normalised name the match is keyed on, shared by the matched campaigns.")
    similarity: float = Field(description="How closely the campaign's own normalised name matches the canonical one, 0 to 1.")
