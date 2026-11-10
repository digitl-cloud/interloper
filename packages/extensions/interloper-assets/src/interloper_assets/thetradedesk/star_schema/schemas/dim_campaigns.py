import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Partner the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name; NULL on rows built from the facts.")
    status: str | None = Field(default=None, description="Configured status; entity rows only.")
    objective: str | None = Field(default=None, description="Type of the campaign's primary goal; entity rows only.")
    campaign_type: str | None = Field(default=None, description="The Trade Desk's campaign type; entity rows only.")
    budget: float | None = Field(
        default=None, description="Total budget, in the advertiser's currency, as are its facts; entity rows only."
    )
    budget_type: str | None = Field(
        default=None, description="lifetime when the campaign sets a budget, else NULL; entity rows only."
    )
    start_date: dt.date | None = Field(
        default=None, description="Scheduled start, as a UTC day like the facts' days; entity rows only."
    )
    end_date: dt.date | None = Field(
        default=None,
        description="Scheduled end, as a UTC day like the facts' days; NULL when open-ended; entity rows only.",
    )
    created_at: dt.datetime | None = Field(default=None, description="When the campaign was created; entity rows only.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    advertiser_id: str | None = Field(
        default=None, description="Advertiser the campaign belongs to; NULL on rows built from the facts."
    )
