import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAdGroups(Schema):
    ad_group_id: str | None = Field(default=None, description="Ad group id, the key the facts join on.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    account_id: str | None = Field(default=None, description="Partner the ad group belongs to.")
    ad_group_name: str | None = Field(default=None, description="Ad group name; NULL on rows built from the facts.")
    status: str | None = Field(default=None, description="Configured status; entity rows only.")
    budget: float | None = Field(
        default=None, description="Ad group budget, in the advertiser's currency, as are its facts; entity rows only."
    )
    budget_type: str | None = Field(
        default=None, description="lifetime when the ad group sets a budget, else NULL; entity rows only."
    )
    created_at: dt.datetime | None = Field(default=None, description="When the ad group was created; entity rows only.")
    updated_at: dt.datetime | None = Field(
        default=None, description="When the ad group was last changed; entity rows only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
