import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DisplayAds(Schema):
    """Sponsored Display product ad snapshot. One row per ad and day, as the extended product ads list returns it."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_id: str | None = Field(default=None, description="Product ad id.")
    ad_group_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    ad_name: str | None = Field(default=None, description="Ad name; ads advertising an ASIN or SKU have none.")
    asin: str | None = Field(default=None, description="ASIN of the advertised product.")
    sku: str | None = Field(default=None, description="SKU of the advertised product; sellers only.")
    landing_page_url: str | None = Field(default=None, description="URL customers land on after a click.")
    landing_page_type: str | None = Field(default=None, description="STORE, MOMENT or OFF_AMAZON_LINK.")
    state: str | None = Field(default=None, description="enabled, paused or archived.")
    serving_status: str | None = Field(default=None, description="Serving status.")
    creation_date: dt.datetime | None = Field(default=None, description="When the ad was created.")
    last_updated_date: dt.datetime | None = Field(default=None, description="When the ad was last changed.")
