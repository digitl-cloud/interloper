import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VendorSalesRetailManufacturingStats(Schema):
    """Retail sales performance per ASIN per day, manufacturing (distributor) view.

    Covers ordered/shipped revenue, units, cost of goods sold and customer returns
    (GET_VENDOR_SALES_REPORT, sellingProgram=RETAIL, distributorView=MANUFACTURING).
    """

    asin: str | None = Field(default=None, description="The ASIN (Amazon Standard Identification Number) of the product")
    start_date: dt.date | None = Field(default=None, description="The start date of the sales data")
    end_date: dt.date | None = Field(default=None, description="The end date of the sales data")
    customer_returns: int | None = Field(default=None, description="The number of customer returns")
    ordered_revenue_amount: float | None = Field(default=None, description="The amount of revenue from ordered items")
    ordered_revenue_currency_code: str | None = Field(
        default=None, description="The currency code for the ordered revenue"
    )
    ordered_units: float | None = Field(default=None, description="The number of units ordered")
    shipped_revenue_amount: float | None = Field(default=None, description="The amount of revenue from shipped items")
    shipped_revenue_currency_code: str | None = Field(
        default=None, description="The currency code for the shipped revenue"
    )
    shipped_cogs_amount: float | None = Field(
        default=None, description="The amount of cost of goods sold for shipped items"
    )
    shipped_cogs_currency_code: str | None = Field(default=None, description="The currency code for the shipped COGS")
    shipped_units: float | None = Field(default=None, description="The number of units shipped")
