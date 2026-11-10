import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ProductsAds(Schema):
    """Sponsored Products ad snapshot. One row per product ad and day, as the v3 product ads list returns it with its extended data."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_id: str | None = Field(default=None, description="Product ad id.")
    ad_group_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    global_ad_id: str | None = Field(default=None, description="Id of the ad across marketplaces.")
    asin: str | None = Field(default=None, description="ASIN of the advertised product; vendors only.")
    sku: str | None = Field(default=None, description="SKU of the advertised product; sellers only.")
    custom_text: str | None = Field(default=None, description="Custom text of a custom text ad.")
    state: str | None = Field(
        default=None, description="ENABLED, PAUSED, ARCHIVED, ENABLING, USER_DELETED, PROPOSED or OTHER."
    )
    global_store_setting_catalog_source_country_code: str | None = Field(
        default=None, description="Country of the marketplace where the seller lists the product."
    )
    extended_data_creation_date_time: dt.datetime | None = Field(default=None, description="When the ad was created.")
    extended_data_last_update_date_time: dt.datetime | None = Field(
        default=None, description="When the ad was last changed."
    )
    extended_data_serving_status: str | None = Field(default=None, description="Serving status.")
    extended_data_serving_status_details: str | None = Field(
        default=None, description="JSON array of the serving status details."
    )
