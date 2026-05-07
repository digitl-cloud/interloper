from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """Pinterest Ads account metadata including ownership and billing details."""

    id: str = Field(description="The unique identifier for the Pinterest Ads account")
    name: str = Field(description="The name of the Pinterest Ads account")
    country: str = Field(description="The country code where the account is registered")
    currency: str = Field(description="The currency code used for billing")
    owner_id: str = Field(description="The unique identifier for the account owner")
    owner_username: str = Field(description="The username of the account owner")
    permissions: str = Field(description="A list of permissions granted to the account")
    created_time: int = Field(description="The timestamp when the account was created")
    updated_time: int = Field(description="The timestamp when the account was last updated")
