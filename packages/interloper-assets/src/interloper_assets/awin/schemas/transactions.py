import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Transactions(Schema):
    """Awin advertiser transaction data including commission, sale amounts, and click attribution."""

    id: int = Field(description="Transaction ID")
    url: str | None = Field(default=None, description="Referring URL")
    advertiser_id: int | None = Field(default=None, description="Advertiser ID")
    publisher_id: int | None = Field(default=None, description="Publisher ID")
    site_name: str | None = Field(default=None, description="Publisher site name")
    commission_status: str | None = Field(default=None, description="Commission status (pending, approved, declined)")
    commission_amount: float | None = Field(default=None, description="Commission amount")
    commission_currency: str | None = Field(default=None, description="Commission currency code")
    old_commission_amount: float | None = Field(default=None, description="Previous commission amount before update")
    old_commission_currency: str | None = Field(default=None, description="Previous commission currency code")
    sale_amount: float | None = Field(default=None, description="Sale amount")
    sale_currency: str | None = Field(default=None, description="Sale currency code")
    old_sale_amount: float | None = Field(default=None, description="Previous sale amount before update")
    old_sale_currency: str | None = Field(default=None, description="Previous sale currency code")
    customer_country: str | None = Field(default=None, description="Customer country code")
    click_date: dt.datetime | None = Field(default=None, description="Date and time of the click")
    transaction_date: dt.datetime | None = Field(default=None, description="Date and time of the transaction")
    validation_date: dt.datetime | None = Field(default=None, description="Date and time of validation")
    type: str | None = Field(default=None, description="Transaction type")
    decline_reason: str | None = Field(default=None, description="Reason for decline if applicable")
    voucher_code_used: bool | None = Field(default=None, description="Whether a voucher code was used")
    voucher_code: str | None = Field(default=None, description="Voucher code used in the transaction")
    amended: bool | None = Field(default=None, description="Whether the transaction was amended")
    amended_reason: str | None = Field(default=None, description="Reason for amendment")
    old_commission_status: str | None = Field(default=None, description="Previous commission status")
    click_device: str | None = Field(default=None, description="Device used for the click")
    transaction_device: str | None = Field(default=None, description="Device used for the transaction")
    ip_hash: str | None = Field(default=None, description="Hashed IP address")
    publisher_url: str | None = Field(default=None, description="Publisher URL")
    advertiser_url: str | None = Field(default=None, description="Advertiser URL")
    order_ref: str | None = Field(default=None, description="Order reference")
    click_ref: str | None = Field(default=None, description="Click reference")
    transaction_parts: str | None = Field(default=None, description="Transaction parts data")
    paid_to_publisher: bool | None = Field(default=None, description="Whether commission was paid to publisher")
    payment_id: int | None = Field(default=None, description="Payment ID")
    transaction_query_id: int | None = Field(default=None, description="Transaction query ID")
    original_sale_amount: float | None = Field(default=None, description="Original sale amount before adjustments")
    original_commission_amount: float | None = Field(
        default=None, description="Original commission amount before adjustments"
    )
    date: dt.date | None = Field(default=None, description="Date of the transaction")
