import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FactCampaignPerformance(Schema):
    """Campaign performance of every Amazon Ads profile and ad product, one row per campaign and day."""

    date: dt.date | None = Field(default=None, description="The report day.")
    profile_id: str | None = Field(default=None, description="Amazon Ads profile the campaign belongs to.")
    ad_product: str | None = Field(
        default=None, description="The ad product: sponsored_products, sponsored_brands or sponsored_display."
    )
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    campaign_name: str | None = Field(default=None, description="Campaign name as reported with the metrics.")
    campaign_status: str | None = Field(default=None, description="Campaign status as reported with the metrics.")
    campaign_budget_amount: float | None = Field(default=None, description="Campaign budget as reported.")
    campaign_budget_currency_code: str | None = Field(default=None, description="Currency of the budget and the cost.")
    impressions: int | None = Field(default=None, description="Times the ads were shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ads.")
    cost: float | None = Field(default=None, description="Amount spent, in the budget currency.")
    purchases: int | None = Field(default=None, description="Purchases attributed within 14 days of a click.")
    sales: float | None = Field(default=None, description="Sales attributed within 14 days of a click.")
