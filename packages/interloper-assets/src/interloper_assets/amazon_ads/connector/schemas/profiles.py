import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Profiles(Schema):
    """Amazon Ads profile snapshot. One row per day for the source's own profile: its marketplace, currency, timezone and account."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    profile_id: int | None = Field(default=None, description="The Amazon Ads profile ID")
    country_code: str | None = Field(default=None, description="The account's country code")
    currency_code: str | None = Field(default=None, description="The account's currency code")
    daily_budget: float | None = Field(default=None, description="The profile's daily budget, in its currency")
    timezone: str | None = Field(default=None, description="The account's timezone")
    account_info_marketplace_string_id: str | None = Field(
        default=None, description="Account info marketplace string ID"
    )
    account_info_id: str | None = Field(default=None, description="Account info id")
    account_info_type: str | None = Field(default=None, description="Account info type: seller, vendor or agency")
    account_info_sub_type: str | None = Field(default=None, description="Account info sub type, such as KDP_AUTHOR")
    account_info_name: str | None = Field(
        default=None, description="Account info name; Amazon does not populate it for sellers"
    )
    account_info_valid_payment_method: bool | None = Field(
        default=None, description="Whether account's payment method is valid; Amazon sends it for vendors only"
    )
