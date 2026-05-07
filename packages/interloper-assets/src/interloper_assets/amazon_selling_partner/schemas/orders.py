import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Orders(Schema):
    """Amazon Selling Partner order data including purchase details, shipping, and financials."""

    amazon_order_id: str | None = Field(default=None, description="Amazon order ID")
    merchant_order_id: str | None = Field(default=None, description="Merchant order ID")
    purchase_date: dt.datetime | None = Field(default=None, description="Purchase date and time")
    last_updated_date: dt.datetime | None = Field(default=None, description="Last update date and time")
    order_status: str | None = Field(default=None, description="Order status")
    fulfillment_channel: str | None = Field(default=None, description="Fulfillment channel (AFN or MFN)")
    sales_channel: str | None = Field(default=None, description="Sales channel")
    ship_service_level: str | None = Field(default=None, description="Ship service level")
    order_total_amount: float | None = Field(default=None, description="Order total amount")
    order_total_currency_code: str | None = Field(default=None, description="Order total currency code")
    number_of_items_shipped: int | None = Field(default=None, description="Number of items shipped")
    number_of_items_unshipped: int | None = Field(default=None, description="Number of items unshipped")
    marketplace_id: str | None = Field(default=None, description="Marketplace ID")
    shipment_service_level_category: str | None = Field(
        default=None, description="Shipment service level category"
    )
    order_type: str | None = Field(default=None, description="Order type")
    earliest_ship_date: dt.datetime | None = Field(default=None, description="Earliest ship date")
    latest_ship_date: dt.datetime | None = Field(default=None, description="Latest ship date")
    earliest_delivery_date: dt.datetime | None = Field(default=None, description="Earliest delivery date")
    latest_delivery_date: dt.datetime | None = Field(default=None, description="Latest delivery date")
    is_business_order: bool | None = Field(default=None, description="Whether this is a business order")
    is_prime: bool | None = Field(default=None, description="Whether this is a Prime order")
    is_premium_order: bool | None = Field(default=None, description="Whether this is a premium order")
    is_global_express_enabled: bool | None = Field(
        default=None, description="Whether global express is enabled"
    )
    is_replacement_order: bool | None = Field(default=None, description="Whether this is a replacement order")
    ship_city: str | None = Field(default=None, description="Shipping city")
    ship_state: str | None = Field(default=None, description="Shipping state or region")
    ship_postal_code: str | None = Field(default=None, description="Shipping postal code")
    ship_country: str | None = Field(default=None, description="Shipping country code")
    date: dt.date | None = Field(default=None, description="Order date")
