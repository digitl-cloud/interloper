import datetime as dt

import interloper as il
from pydantic import Field


class CampaignMatches(il.Schema):
    date: dt.date | None = Field(default=None, description="The day the snapshot was taken (stamped from the partition).")
    source_key: str = Field(description="The catalog key of the connector source the campaign came from.")
    source_id: str = Field(description="The instance id of the connector source the campaign came from.")
    campaign_id: str = Field(description="The upstream campaign's id, read from either connector schema's id or campaign_id field.")
    campaign_name: str = Field(description="The upstream campaign's name, read from either connector schema's name or campaign_name field.")
    canonical_name: str = Field(description="The campaign name lower-cased and stripped, used as the placeholder match key across sources.")
    similarity: float = Field(description="Placeholder match confidence; always 1.0 until real fuzzy matching replaces the name-equality logic.")
