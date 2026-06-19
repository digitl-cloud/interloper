import datetime

from interloper.schema import Schema
from pydantic import Field


class ProductsGrossAndInvalidTrafficStats(Schema):
    """Products traffic quality metrics including gross impressions, invalid click-throughs, and invalid impressions."""

    campaign_name: str | None = Field(..., description="The name of the campaign")
    campaign_status: str | None = Field(..., description="The status of the campaign")
    clicks: float | None = Field(..., description="The number of clicks")
    date: datetime.date | None = Field(..., description="The date of the data entry")
    gross_click_throughs: float | None = Field(..., description="The number of gross click throughs")
    gross_impressions: float | None = Field(..., description="The number of gross impressions")
    impressions: float | None = Field(..., description="The number of impressions")
    invalid_click_through_rate: float | None = Field(..., description="The rate of invalid click throughs")
    invalid_click_throughs: float | None = Field(..., description="The number of invalid click throughs")
    invalid_impression_rate: float | None = Field(..., description="The rate of invalid impressions")
    invalid_impressions: float | None = Field(..., description="The number of invalid impressions")
