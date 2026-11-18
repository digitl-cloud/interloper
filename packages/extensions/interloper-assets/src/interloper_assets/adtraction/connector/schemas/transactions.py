import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Transactions(Schema):
    """Transactions registered at the advertiser's website, one row per transaction, whatever its status."""

    click_ad_id: str | None = Field(
        default=None,
        description="Numerical Adtraction ID of the banner or link of the click that preceded the transaction",
    )
    click_ad_name: str | None = Field(default=None, description="Name of the banner or link of the click")
    click_ad_type: str | None = Field(default=None, description="Type of the banner or link of the click")
    click_channel_id: str | None = Field(
        default=None, description="Numerical Adtraction ID of the channel of the click"
    )
    click_channel_name: str | None = Field(default=None, description="Name of the channel of the click")
    click_click_date: dt.datetime | None = Field(default=None, description="When the click happened")
    click_status: str | None = Field(
        default=None, description="Status of the click: Fresh while within the cookie duration, Stale after it"
    )
    cost: float | None = Field(
        default=None, description="Cost of the transaction, partner commission and Adtraction platform fee included"
    )
    currency: str | None = Field(default=None, description="Currency of the transaction's amounts, in ISO 4217 format")
    payment_status: int | None = Field(
        default=None,
        description="Payment status: 1 not invoiced, 2 invoiced to the advertiser, 3 ready for payment, 4 paid to the partner",
    )
    order_id: str | None = Field(default=None, description="The advertiser's order ID")
    order_value: float | None = Field(default=None, description="Order value of the transaction")
    reason_text: str | None = Field(
        default=None, description="Why the transaction was rejected; only on rejected transactions"
    )
    transaction_date: dt.datetime | None = Field(
        default=None, description="When the advertiser's website registered the transaction"
    )
    last_updated: dt.datetime | None = Field(
        default=None, description="When the transaction's status or order value last changed; only once one has"
    )
    auto_approval_date: dt.date | None = Field(
        default=None,
        description="The day a pending transaction is approved automatically unless approved or rejected before",
    )
    transaction_name: str | None = Field(default=None, description="Name of the transaction")
    transaction_status: int | None = Field(
        default=None, description="Status: 1 approved, 2 pending, 4 open claim, 5 rejected"
    )
    unique_id: str | None = Field(default=None, description="Unique ID of the transaction")
    device: str | None = Field(default=None, description="Type of device the transaction was completed on")
    operating_system: str | None = Field(
        default=None, description="Operating system of the device the transaction was completed on"
    )
    geo_location: str | None = Field(
        default=None, description="Market the transaction was completed in, as an ISO 3166-1 alpha-2 country code"
    )
    product_category_data: str | None = Field(
        default=None,
        description="The transaction's product categories with their order value and cost, as JSON; only with product category tracking",
    )
    date: dt.date | None = Field(
        default=None,
        description="The day the transaction list was requested for, in Stockholm time (stamped from the partition).",
    )
