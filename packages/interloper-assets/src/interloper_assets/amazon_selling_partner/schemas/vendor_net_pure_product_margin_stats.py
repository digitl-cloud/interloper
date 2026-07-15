import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VendorNetPureProductMarginStats(Schema):
    """Net pure product margin per ASIN per day (GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT)."""

    asin: str | None = Field(default=None, description="Amazon Standard Identification Number for the product.")
    start_date: dt.date | None = Field(default=None, description="The start date of the reporting period.")
    end_date: dt.date | None = Field(default=None, description="The end date of the reporting period.")
    net_pure_product_margin: float | None = Field(
        default=None, description="The net pure product margin for the product."
    )
