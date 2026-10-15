import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimCampaigns(Schema):
    """One row per Amazon Ads campaign with its ad product and profile, from the facts."""

    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    ad_product: str | None = Field(
        default=None, description="The ad product: sponsored_products, sponsored_brands or sponsored_display."
    )
    profile_id: str | None = Field(default=None, description="Amazon Ads profile the campaign belongs to.")
    campaign_status: str | None = Field(default=None, description="Campaign status on its latest report day.")
    campaign_budget_amount: float | None = Field(default=None, description="Campaign budget on its latest report day.")
    campaign_budget_currency_code: str | None = Field(default=None, description="Currency of the budget.")
    date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
