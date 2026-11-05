import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Accounts(Schema):
    """Bing Ads account snapshot. One row per day for the source's own account, as Customer Management's GetAccount returns it, without its business address and tax registration."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    bill_to_customer_id: str | None = Field(default=None, description="Customer billed for the account.")
    currency_code: str | None = Field(default=None, description="ISO code of the account's currency.")
    account_financial_status: str | None = Field(
        default=None, description="Financial status (ClearFinancialStatus, CreditWarning, Hold...)."
    )
    id: str | None = Field(default=None, description="Account id.")
    language: str | None = Field(default=None, description="Language of the account's communications.")
    last_modified_by_user_id: str | None = Field(default=None, description="User who last changed the account.")
    last_modified_time: dt.datetime | None = Field(
        default=None, description="When the account was last changed, in UTC."
    )
    name: str | None = Field(default=None, description="Account name.")
    number: str | None = Field(default=None, description="Account number, as the Bing Ads UI shows it.")
    parent_customer_id: str | None = Field(default=None, description="Customer (manager) the account belongs to.")
    payment_method_id: str | None = Field(default=None, description="Payment method the account is charged to.")
    payment_method_type: str | None = Field(
        default=None, description="Kind of payment method (CreditCard, Invoice...)."
    )
    primary_user_id: str | None = Field(default=None, description="The account's primary user.")
    account_life_cycle_status: str | None = Field(
        default=None, description="Account status (Active, Inactive, Pause, Pending, Suspended, Draft)."
    )
    time_stamp: str | None = Field(default=None, description="Base64 version stamp Bing uses for concurrent updates.")
    time_zone: str | None = Field(
        default=None, description="Bing time zone of the account, such as AmsterdamBerlinBernRomeStockholmVienna."
    )
    pause_reason: int | None = Field(default=None, description="Why a paused account is paused, as Bing codes it.")
    linked_agencies: str | None = Field(
        default=None, description="JSON array of the agencies (Id, Name) linked to the account."
    )
    sales_house_customer_id: str | None = Field(
        default=None, description="Customer of the sales house managing the account."
    )
    back_up_payment_instrument_id: str | None = Field(default=None, description="Backup payment instrument.")
    billing_threshold_amount: float | None = Field(
        default=None, description="Billing threshold, in the account's currency."
    )
    auto_tag_type: str | None = Field(
        default=None, description="Auto-tagging of the MSCLKID click id (Inactive, Preserve, Replace)."
    )
    sold_to_payment_instrument_id: str | None = Field(
        default=None, description="Payment instrument the account is sold to."
    )
