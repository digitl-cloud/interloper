import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Advertiser id, the key the facts join on.")
    account_name: str | None = Field(default=None, description="NULL until an advertisers entity is collected.")
    currency: str | None = Field(default=None, description="Currency the reports state their amounts in.")
    timezone: str | None = Field(default=None, description="NULL until an advertisers entity is collected.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
