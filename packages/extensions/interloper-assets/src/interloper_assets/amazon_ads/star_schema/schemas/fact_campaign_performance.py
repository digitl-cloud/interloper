import datetime as dt

from interloper import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    date: dt.date | None = Field(default=None, description="The report day, in the profile's timezone.")
    account_id: str | None = Field(default=None, description="Profile id, the key the account dimension joins on.")
    campaign_id: str | None = Field(default=None, description="Campaign id, the key the campaign dimension joins on.")
    impressions: int | None = Field(default=None, description="Times the ads were shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ads.")
    cost: float | None = Field(default=None, description="Amount spent, in the profile's currency.")
    currency: str | None = Field(default=None, description="Currency of every amount in the row.")
    conversions: int | None = Field(
        default=None,
        description="Purchases attributed within 14 days of a click, as each ad product's campaign report counts them.",
    )
    conversion_value: float | None = Field(
        default=None, description="Sales attributed within 14 days of a click, in the profile's currency."
    )
    purchases: int | None = Field(
        default=None,
        description="Purchases attributed within 14 days of a click, as each ad product's campaign report counts them.",
    )
    revenue: float | None = Field(
        default=None, description="Sales attributed within 14 days of a click, in the profile's currency."
    )
    campaign_type: str | None = Field(
        default=None, description="The ad product: sponsored_products, sponsored_brands or sponsored_display."
    )
