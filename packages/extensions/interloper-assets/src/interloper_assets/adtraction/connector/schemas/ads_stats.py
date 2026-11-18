import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsStats(Schema):
    """Performance of each banner or link per day, counting approved and pending transactions."""

    ad_id: str | None = Field(default=None, description="Numerical Adtraction ID of the banner or link")
    ad_name: str | None = Field(default=None, description="Name of the banner or link")
    clicks: int | None = Field(default=None, description="Number of clicks")
    clicks_unique: int | None = Field(default=None, description="Number of unique clicks")
    cost: float | None = Field(
        default=None,
        description="Cost of the transactions, partner commission and Adtraction platform fee included, in the program's currency",
    )
    impressions: int | None = Field(default=None, description="Number of banner and link impressions")
    impressions_unique: int | None = Field(default=None, description="Number of unique banner and link impressions")
    order_value: float | None = Field(
        default=None, description="Order value of the sale transactions, in the program's currency"
    )
    leads: int | None = Field(default=None, description="Number of lead transactions")
    sales: int | None = Field(default=None, description="Number of sale transactions")
    ctr: float | None = Field(default=None, description="Click-through ratio: clicks divided by impressions")
    lead_ratio: float | None = Field(default=None, description="Lead ratio: leads divided by unique clicks")
    conversion_ratio: float | None = Field(default=None, description="Conversion ratio: sales divided by unique clicks")
    aov: float | None = Field(default=None, description="Average order value: order value divided by sale transactions")
    date: dt.date | None = Field(
        default=None, description="The day the report was requested for (stamped from the partition)."
    )
