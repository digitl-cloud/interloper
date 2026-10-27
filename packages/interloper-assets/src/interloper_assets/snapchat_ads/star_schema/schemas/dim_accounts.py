import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Ad account id, the key the facts join on.")
    account_name: str | None = Field(
        default=None, description="Ad account name; NULL until an ad accounts entity is collected."
    )
    currency: str | None = Field(default=None, description="Currency of the account's amounts.")
    timezone: str | None = Field(
        default=None,
        description="IANA timezone the account's days are reported in; NULL until an ad accounts entity is collected.",
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
