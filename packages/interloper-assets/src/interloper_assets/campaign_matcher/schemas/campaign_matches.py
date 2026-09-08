import datetime as dt

import interloper as il


class CampaignMatches(il.Schema):
    date: dt.date | None
    source_key: str
    source_id: str
    campaign_id: str
    campaign_name: str
    canonical_name: str
    similarity: float
