import datetime as dt

from interloper import Schema
from pydantic import Field


class DimAds(Schema):
    ad_id: str | None = Field(default=None, description="Ad id.")
    ad_group_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    account_id: str | None = Field(default=None, description="Profile the ad belongs to.")
    ad_name: str | None = Field(
        default=None,
        description="Ad name, where Amazon names the ad: Sponsored Brands ads and some Sponsored Display ads; "
        "a product ad is known by its ASIN or SKU.",
    )
    status: str | None = Field(default=None, description="Configured state, upper case; entity rows only.")
    created_at: dt.datetime | None = Field(default=None, description="When the ad was created; entity rows only.")
    updated_at: dt.datetime | None = Field(default=None, description="When the ad was last changed; entity rows only.")
    as_of_date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
    asin: str | None = Field(default=None, description="ASIN of the advertised product, for product ads.")
    sku: str | None = Field(default=None, description="SKU of the advertised product, for sellers' product ads.")
