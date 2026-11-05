import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Account id, the key the facts join on.")
    account_name: str | None = Field(default=None, description="Account name.")
    currency: str | None = Field(default=None, description="Currency of the account's amounts.")
    timezone: str | None = Field(
        default=None,
        description="IANA zone of the account's Bing time zone; NULL on rows built from the report or the facts.",
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    customer_id: str | None = Field(default=None, description="Customer (manager) the account belongs to.")
