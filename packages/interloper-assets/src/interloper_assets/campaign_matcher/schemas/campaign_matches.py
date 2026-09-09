import datetime as dt

import interloper as il
from pydantic import Field


class CampaignMatches(il.Schema):
    date: dt.date | None = Field(default=None, description="Snapshot day, stamped from the run's partition.")
    campaign_id: str = Field(description="The campaign's id in its advertising platform.")
    campaign_name: str = Field(description="The campaign's name as its advertising platform reports it.")
    canonical_name: str = Field(description="The name campaigns are matched on: lower-cased and trimmed of whitespace.")
    similarity: float = Field(description="Confidence of the match between the campaign's name and its canonical name, 0 to 1.")
