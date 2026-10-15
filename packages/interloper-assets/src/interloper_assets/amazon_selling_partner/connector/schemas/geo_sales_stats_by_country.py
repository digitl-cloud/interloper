import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class GeoSalesStatsByCountry(Schema):
    """Shipped sales and cost metrics per ASIN per ship-to country per day (Data Kiosk sourcingView)."""

    date: dt.date | None = Field(default=None, description="The day the metrics are aggregated over.")
    asin: str | None = Field(default=None, description="Amazon Standard Identification Number for the product.")
    ship_to_country_code: str | None = Field(
        default=None, description="Customer country code for a shipment in ISO 3166-1 alpha-2 format"
    )
    shipped_units_with_revenue_value_amount: float | None = Field(
        default=None, description="The value of associated gross price the customer paid for shipped units"
    )
    shipped_units_with_revenue_value_currency_code: str | None = Field(
        default=None, description="The currency unit of associated gross price the customer paid for shipped units"
    )
    shipped_units_with_revenue_units: int | None = Field(default=None, description="The number of shipped items")
    net_ppm: float | None = Field(
        default=None,
        description="Net Pure Product Margin: Amazon's margin after accounting for CCOGs and sales discounts",
    )
    shipped_cogs_amount: float | None = Field(
        default=None, description="The shipped cost of goods sold, the price Amazon paid a vendor per item procurement"
    )
    shipped_cogs_currency_code: str | None = Field(default=None, description="Currency code for shipped_cogs_amount")
    contra_cogs_per_unit_amount: float | None = Field(
        default=None, description="The average Vendor Contra-COGS (VFCC) of shipped items, per shipped unit"
    )
    contra_cogs_per_unit_currency_code: str | None = Field(
        default=None, description="Currency code for contra_cogs_per_unit_amount"
    )
    contra_cogs_amount: float | None = Field(
        default=None,
        description="The total Contra Cost of Goods Sold (COGS) realized by outbound shipments minus Display Ads CCOGs",
    )
    contra_cogs_currency_code: str | None = Field(default=None, description="Currency code for contra_cogs_amount")
    average_sales_discount_amount: float | None = Field(
        default=None, description="The average selling discount per unit of shipped items"
    )
    average_sales_discount_currency_code: str | None = Field(
        default=None, description="Currency code for average_sales_discount_amount"
    )
