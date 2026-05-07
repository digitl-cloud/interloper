from interloper.schema import Schema
from pydantic import Field


class Profiles(Schema):
    profile_id: int = Field(..., description="The Amazon Ads profile ID")
    country_code: str = Field(..., description="The account's country code")
    currency_code: str = Field(..., description="The account's currency code")
    timezone: str = Field(..., description="The account's timezone")
    account_info_marketplace_string_id: str = Field(..., description="Account info marketplace string ID")
    account_info_id: str = Field(..., description="Account info id")
    account_info_type: str = Field(..., description="Account info type")
    account_info_name: str = Field(..., description="Account info name")
    account_info_valid_payment_method: bool = Field(..., description="Whether account's payment method is valid")
