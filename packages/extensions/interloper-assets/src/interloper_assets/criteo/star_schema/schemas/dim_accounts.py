import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Advertiser id, the key the facts join on.")
    account_name: str | None = Field(default=None, description="Advertiser name; entity rows only.")
    currency: str | None = Field(
        default=None, description="Currency of the advertiser's bids and budgets, and of its reports' amounts."
    )
    timezone: str | None = Field(
        default=None,
        description="IANA time zone the advertiser's report days are in; entity rows only, NULL when Criteo states "
        "none (its reports are then in UTC).",
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
