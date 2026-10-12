import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimAccounts(Schema):
    """One row per Facebook Ads account, from the facts."""

    account_id: str | None = Field(default=None, description="Ad account id, the key the facts join on.")
    account_name: str | None = Field(default=None, description="Ad account name.")
    account_currency: str | None = Field(default=None, description="Currency of the account's amounts.")
    date: dt.date | None = Field(default=None, description="Latest report day the account appears in.")
