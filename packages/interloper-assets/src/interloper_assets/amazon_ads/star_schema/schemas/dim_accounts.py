import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimAccounts(Schema):
    """One row per Amazon Ads profile, from its latest entity row, else the facts."""

    profile_id: str | None = Field(default=None, description="Profile id, the key the facts join on.")
    account_name: str | None = Field(
        default=None, description="Name of the advertising account, known from entity rows only."
    )
    account_type: str | None = Field(
        default=None, description="Account type (seller, vendor, agency), known from entity rows only."
    )
    marketplace_id: str | None = Field(
        default=None, description="Marketplace the profile advertises in, known from entity rows only."
    )
    country_code: str | None = Field(
        default=None, description="Country of the marketplace, known from entity rows only."
    )
    currency_code: str | None = Field(default=None, description="Currency of the profile's amounts.")
    timezone: str | None = Field(
        default=None, description="Timezone the profile reports in, known from entity rows only."
    )
    date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
