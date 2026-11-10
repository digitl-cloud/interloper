import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Partner id, the key the facts join on.")
    currency: str | None = Field(
        default=None,
        description="The partner's own currency; each advertiser bills in its own, which every fact row states.",
    )
    timezone: str | None = Field(
        default=None, description="UTC: The Trade Desk reports, paces and bills every day in UTC."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
