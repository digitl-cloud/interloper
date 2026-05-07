import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Settlements(Schema):
    """Amazon Selling Partner settlement data including fees, refunds, and disbursements."""

    settlement_id: str | None = Field(default=None, description="Settlement ID")
    settlement_start_date: dt.datetime | None = Field(default=None, description="Settlement period start date")
    settlement_end_date: dt.datetime | None = Field(default=None, description="Settlement period end date")
    deposit_date: dt.datetime | None = Field(default=None, description="Deposit date")
    total_amount: float | None = Field(default=None, description="Total settlement amount")
    currency: str | None = Field(default=None, description="Settlement currency code")
    transaction_type: str | None = Field(default=None, description="Transaction type")
    order_id: str | None = Field(default=None, description="Order ID")
    merchant_order_id: str | None = Field(default=None, description="Merchant order ID")
    adjustment_id: str | None = Field(default=None, description="Adjustment ID")
    shipment_id: str | None = Field(default=None, description="Shipment ID")
    marketplace_name: str | None = Field(default=None, description="Marketplace name")
    amount_type: str | None = Field(default=None, description="Amount type (ItemPrice, Promotion, etc.)")
    amount_description: str | None = Field(default=None, description="Amount description")
    amount: float | None = Field(default=None, description="Amount")
    fulfillment_id: str | None = Field(default=None, description="Fulfillment ID")
    posted_date: dt.datetime | None = Field(default=None, description="Posted date")
    posted_date_time: dt.datetime | None = Field(default=None, description="Posted date and time")
    order_item_code: str | None = Field(default=None, description="Order item code")
    merchant_order_item_id: str | None = Field(default=None, description="Merchant order item ID")
    merchant_adjustment_item_id: str | None = Field(default=None, description="Merchant adjustment item ID")
    sku: str | None = Field(default=None, description="SKU")
    quantity_purchased: int | None = Field(default=None, description="Quantity purchased")
    promotion_id: str | None = Field(default=None, description="Promotion ID")
    date: dt.date | None = Field(default=None, description="Settlement date")
