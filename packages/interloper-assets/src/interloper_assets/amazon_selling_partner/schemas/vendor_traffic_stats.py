import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VendorTrafficStats(Schema):
    """Glance views per ASIN per day on the Amazon marketplace (GET_VENDOR_TRAFFIC_REPORT)."""

    asin: str | None = Field(default=None, description="The ASIN (Amazon Standard Identification Number) of the product")
    start_date: dt.date | None = Field(default=None, description="The start date of the traffic data")
    end_date: dt.date | None = Field(default=None, description="The end date of the traffic data")
    glance_views: int | None = Field(
        default=None, description="The number of times the product was viewed on the Amazon marketplace"
    )
