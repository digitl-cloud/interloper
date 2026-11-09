import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Advertiser id, the key the facts join on.")
    account_name: str | None = Field(default=None, description="Advertiser name; entity rows only.")
    currency: str | None = Field(
        default=None, description="Currency the advertiser's reports state their amounts in, on its latest report day."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
