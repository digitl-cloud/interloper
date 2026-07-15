import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VendorSalesRetailSourcingStats(Schema):
    """Retail sales performance per ASIN per day, sourcing (distributor) view.

    Covers shipped revenue, units, cost of goods sold and customer returns
    (GET_VENDOR_SALES_REPORT, sellingProgram=RETAIL, distributorView=SOURCING).
    """

    asin: str | None = Field(default=None, description="The ASIN (Amazon Standard Identification Number) of the product")
    start_date: dt.date | None = Field(default=None, description="The start date of the sales data")
    end_date: dt.date | None = Field(default=None, description="The end date of the sales data")
    customer_returns: int | None = Field(default=None, description="The number of customer returns")
    shipped_revenue_amount: float | None = Field(
        default=None, description="The amount of revenue generated from shipped items"
    )
    shipped_revenue_currency_code: str | None = Field(
        default=None, description="The currency code for the revenue amount"
    )
    shipped_cogs_amount: float | None = Field(
        default=None, description="The amount of cost of goods sold (COGS) for shipped items"
    )
    shipped_cogs_currency_code: str | None = Field(
        default=None, description="The currency code for the cost of goods sold (COGS) amount"
    )
    shipped_units: float | None = Field(default=None, description="The number of units shipped")
