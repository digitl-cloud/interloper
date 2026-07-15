import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ProductFulfillmentStats(Schema):
    """Vendor sourcing and fulfillment metrics per ASIN per day (Data Kiosk sourcingView)."""

    date: dt.date | None = Field(default=None, description="The day the metrics are aggregated over.")
    asin: str | None = Field(default=None, description="Amazon Standard Identification Number for the product.")
    customer_returns: int | None = Field(
        default=None, description="The number of items returned as recorded on the Customer Returns transaction table"
    )
    open_purchase_order_quantity: int | None = Field(
        default=None,
        description="Number of products in the vendor's open POs (confirmed PO quantities not yet received)",
    )
    confirmed_units: int | None = Field(
        default=None, description="Number of purchase order units confirmed to be shipped to Amazon"
    )
    most_recent_submitted: int | None = Field(
        default=None, description="The number of units Amazon has ordered in the most recent purchase orders of the week"
    )
    net_received_units: int | None = Field(
        default=None,
        description="Net number of units received by Amazon after subtracting the units returned to the supplier",
    )
    net_received_value_amount: float | None = Field(
        default=None,
        description="Net cost of units received by Amazon after subtracting the units returned to the supplier",
    )
    net_received_value_currency_code: str | None = Field(
        default=None, description="Net cost currency code of units received by Amazon"
    )
    overall_vendor_lead_time: float | None = Field(
        default=None, description="Time (in days) from when a vendor receives a PO to when inventory is received in a FC"
    )
    procurable_product_oos: float | None = Field(
        default=None, description="Out of stock rate on all products that are procurable"
    )
    received_fill_rate: float | None = Field(
        default=None, description="PO units received by Amazon compared to PO units confirmed by the vendor"
    )
    vendor_confirmation_rate: float | None = Field(
        default=None, description="How many units vendors confirm out of the units Amazon asks for"
    )
