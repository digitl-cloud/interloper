import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PublishersStats(Schema):
    """Awin advertiser report by publisher with performance metrics."""

    date: dt.date | None = Field(
        default=None, description="The day the report was requested for (stamped from the partition)."
    )
    publisher_id: str | None = Field(default=None, description="Publisher ID")
    publisher_name: str | None = Field(default=None, description="Publisher name")
    impressions: int | None = Field(default=None, description="Number of impressions")
    clicks: int | None = Field(default=None, description="Number of clicks")
    total_commission: float | None = Field(default=None, description="Total commission amount")
    total_sales_amount: float | None = Field(default=None, description="Total sales amount")
    total_number_of_transactions: int | None = Field(default=None, description="Total number of transactions")
    total_number_of_leads: int | None = Field(default=None, description="Total number of leads")
    total_number_of_bounties: int | None = Field(default=None, description="Total number of bounties")
    currency: str | None = Field(default=None, description="Currency code")
