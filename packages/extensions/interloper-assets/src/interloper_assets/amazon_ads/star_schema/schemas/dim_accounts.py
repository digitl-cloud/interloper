import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAccounts(Schema):
    account_id: str | None = Field(default=None, description="Profile id, the key the facts join on.")
    account_name: str | None = Field(
        default=None, description="Name of the advertising account, known from the profile entity only."
    )
    currency: str | None = Field(default=None, description="Currency of the profile's amounts.")
    timezone: str | None = Field(
        default=None, description="Timezone the profile reports in, known from the profile entity only."
    )
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    account_type: str | None = Field(
        default=None, description="Account type (seller, vendor, agency), known from the profile entity only."
    )
    marketplace_id: str | None = Field(
        default=None, description="Marketplace the profile advertises in, known from the profile entity only."
    )
    country_code: str | None = Field(
        default=None, description="Country of the marketplace, known from the profile entity only."
    )
