import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CustomerClients(Schema):
    """Search Ads 360 client accounts, direct and indirect, under the manager account."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    customer_client_currency_code: str | None = Field(
        default=None, description="The ISO 4217 currency code of the client account."
    )
    customer_client_descriptive_name: str | None = Field(
        default=None, description="The descriptive name of the client account."
    )
    customer_client_id: str | None = Field(default=None, description="The ID of the client account.")
    customer_client_level: int | None = Field(
        default=None, description="The distance from the manager account to the client (0 for the manager itself)."
    )
    customer_client_manager: bool | None = Field(
        default=None, description="Whether the client account is itself a manager account."
    )
    customer_client_resource_name: str | None = Field(
        default=None, description="The resource name of the customer client link."
    )
    customer_client_status: str | None = Field(
        default=None, description="The status of the client account (e.g. ENABLED, CANCELED)."
    )
    customer_descriptive_name: str | None = Field(
        default=None, description="The descriptive name of the manager account."
    )
    customer_id: str | None = Field(default=None, description="The ID of the manager account.")
    customer_resource_name: str | None = Field(default=None, description="The resource name of the manager account.")
