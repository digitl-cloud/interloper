import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Advertiser id, the key the facts join on.")
    currency: str | None = Field(default=None, description="Currency the reports state their amounts in.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
