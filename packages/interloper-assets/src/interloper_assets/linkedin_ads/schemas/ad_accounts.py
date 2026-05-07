from interloper.schema import Schema
from pydantic import Field


class AdAccounts(Schema):
    """LinkedIn Ads account metadata including name, currency, and status."""

    id: int = Field(description="The unique identifier of the ad account")
    name: str = Field(description="The name of the ad account")
    currency: str = Field(description="The currency used in the ad account")
    status: str = Field(description="The status of the ad account")
