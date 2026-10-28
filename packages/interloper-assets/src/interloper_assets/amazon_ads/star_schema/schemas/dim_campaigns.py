import datetime as dt

from interloper import Schema
from pydantic import Field


class DimCampaigns(Schema):
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    account_id: str | None = Field(default=None, description="Profile the campaign belongs to.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    status: str | None = Field(default=None, description="Campaign status as the campaign report carries it.")
    campaign_type: str | None = Field(
        default=None, description="The ad product: sponsored_products, sponsored_brands or sponsored_display."
    )
    budget: float | None = Field(default=None, description="Campaign budget, in the profile's currency.")
    budget_type: str | None = Field(default=None, description="Budget type as the campaign report carries it.")
    start_date: dt.date | None = Field(
        default=None, description="NULL until a campaigns entity is collected; the campaign reports carry no schedule."
    )
    end_date: dt.date | None = Field(
        default=None, description="NULL until a campaigns entity is collected; the campaign reports carry no schedule."
    )
    created_at: dt.datetime | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    updated_at: dt.datetime | None = Field(default=None, description="NULL until a campaigns entity is collected.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
