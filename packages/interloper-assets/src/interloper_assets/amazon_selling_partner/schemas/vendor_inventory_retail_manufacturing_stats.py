import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class VendorInventoryRetailManufacturingStats(Schema):
    """Retail inventory health per ASIN per day, manufacturing (distributor) view.

    Covers on-hand/received/aged inventory cost and units, lead time, fill rate,
    sell-through and out-of-stock rates
    (GET_VENDOR_INVENTORY_REPORT, sellingProgram=RETAIL, distributorView=MANUFACTURING).
    """

    asin: str | None = Field(default=None, description="The ASIN (Amazon Standard Identification Number) of the product")
    start_date: dt.date | None = Field(default=None, description="The start date of the inventory data")
    end_date: dt.date | None = Field(default=None, description="The end date of the inventory data")
    aged_90_plus_days_sellable_inventory_cost_amount: float | None = Field(
        default=None, description="The cost amount of aged 90+ days sellable inventory"
    )
    aged_90_plus_days_sellable_inventory_cost_currency_code: str | None = Field(
        default=None, description="The currency code of the cost amount of aged 90+ days sellable inventory"
    )
    aged_90_plus_days_sellable_inventory_units: float | None = Field(
        default=None, description="The number of units in aged 90+ days sellable inventory"
    )
    average_vendor_lead_time_days: float | None = Field(
        default=None, description="The average lead time in days for vendors"
    )
    net_received_inventory_cost_amount: float | None = Field(
        default=None, description="The cost amount of net received inventory"
    )
    net_received_inventory_cost_currency_code: str | None = Field(
        default=None, description="The currency code of the cost amount of net received inventory"
    )
    net_received_inventory_units: float | None = Field(
        default=None, description="The number of units in net received inventory"
    )
    open_purchase_order_units: float | None = Field(
        default=None, description="The number of units in open purchase orders"
    )
    procurable_product_out_of_stock_rate: float | None = Field(
        default=None, description="The rate at which procurable products are out of stock"
    )
    receive_fill_rate: float | None = Field(default=None, description="The fill rate of received inventory")
    sell_through_rate: float | None = Field(default=None, description="The rate at which products are sold through")
    sellable_on_hand_inventory_cost_amount: float | None = Field(
        default=None, description="The cost amount of sellable on-hand inventory"
    )
    sellable_on_hand_inventory_cost_currency_code: str | None = Field(
        default=None, description="The currency code of the cost amount of sellable on-hand inventory"
    )
    sellable_on_hand_inventory_units: float | None = Field(
        default=None, description="The number of units in sellable on-hand inventory"
    )
    sourceable_product_out_of_stock_rate: float | None = Field(
        default=None, description="The rate at which sourceable products are out of stock"
    )
    uft: int | None = Field(default=None, description="The UFT (Unfulfillable) value")
    unfilled_customer_ordered_units: float | None = Field(
        default=None, description="The number of units in unfilled customer orders"
    )
    unhealthy_inventory_cost: float | None = Field(default=None, description="The cost of unhealthy inventory")
    unhealthy_inventory_units: float | None = Field(
        default=None, description="The number of units in unhealthy inventory"
    )
    unsellable_on_hand_inventory_cost_amount: float | None = Field(
        default=None, description="The cost amount of unsellable on-hand inventory"
    )
    unsellable_on_hand_inventory_cost_currency_code: str | None = Field(
        default=None, description="The currency code of the cost amount of unsellable on-hand inventory"
    )
    unsellable_on_hand_inventory_units: float | None = Field(
        default=None, description="The number of units in unsellable on-hand inventory"
    )
    vendor_confirmation_rate: float | None = Field(
        default=None, description="The rate at which vendor confirmations are received"
    )
