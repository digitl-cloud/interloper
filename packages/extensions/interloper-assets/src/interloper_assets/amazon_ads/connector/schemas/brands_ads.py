import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class BrandsAds(Schema):
    """Sponsored Brands ad snapshot. One row per ad and day, as the v4 ads list returns it."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_id: str | None = Field(default=None, description="Ad id.")
    ad_group_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    name: str | None = Field(default=None, description="Ad name; ads of version 3 campaigns have none.")
    state: str | None = Field(default=None, description="ENABLED, PAUSED, ARCHIVED or PROPOSED.")
    creative_type: str | None = Field(
        default=None,
        description="PRODUCT_COLLECTION, STORE_SPOTLIGHT, VIDEO, BRAND_VIDEO, AUTO_COLLECTION or MANUAL_COLLECTION.",
    )
    creative_asins: str | None = Field(default=None, description="JSON array of the advertised ASINs.")
    creative_asin_exclusions: str | None = Field(default=None, description="JSON array of the excluded ASINs.")
    creative_companion_asins: str | None = Field(default=None, description="JSON array of the companion ASINs.")
    creative_brand_name: str | None = Field(default=None, description="Brand name shown in the ad.")
    creative_brand_logo_asset_id: str | None = Field(default=None, description="Asset id of the brand logo.")
    creative_brand_logo_url: str | None = Field(default=None, description="URL of the brand logo.")
    creative_brand_logo_crop: str | None = Field(
        default=None, description="JSON object of the crop applied to the brand logo."
    )
    creative_custom_image_asset_id: str | None = Field(default=None, description="Asset id of the custom image.")
    creative_custom_image_url: str | None = Field(default=None, description="URL of the custom image.")
    creative_custom_image_crop: str | None = Field(
        default=None, description="JSON object of the crop applied to the custom image."
    )
    creative_custom_images: str | None = Field(default=None, description="JSON array of the custom images.")
    creative_headline: str | None = Field(default=None, description="Headline text.")
    creative_headlines: str | None = Field(default=None, description="JSON array of the headline texts.")
    creative_original_headline: str | None = Field(
        default=None, description="Headline as the advertiser submitted it, before translation."
    )
    creative_original_headlines: str | None = Field(
        default=None, description="JSON array of the headlines as the advertiser submitted them."
    )
    creative_title: str | None = Field(default=None, description="Creative title.")
    creative_collection_name: str | None = Field(default=None, description="Name of the product collection.")
    creative_collection_type: str | None = Field(default=None, description="BUNDLE or THEMED.")
    creative_subpages: str | None = Field(default=None, description="JSON array of the store spotlight subpages.")
    creative_video_asset_ids: str | None = Field(default=None, description="JSON array of the video asset ids.")
    creative_original_video_asset_ids: str | None = Field(
        default=None, description="JSON array of the video asset ids as submitted, before translation."
    )
    creative_consent_to_translate: bool | None = Field(
        default=None, description="Whether Amazon may translate the headline and video."
    )
    creative_creative_properties_to_optimize: str | None = Field(
        default=None, description="JSON array of the creative properties Amazon may enhance."
    )
    creative_creative_status: str | None = Field(default=None, description="Moderation status of the creative.")
    creative_creative_version: str | None = Field(default=None, description="Version of the submitted creative.")
    landing_page_page_type: str | None = Field(
        default=None, description="STORE, PRODUCT_LIST, DETAIL_PAGE or CUSTOM_URL."
    )
    landing_page_url: str | None = Field(default=None, description="URL of the landing page.")
    landing_page_asins: str | None = Field(default=None, description="JSON array of the landing page ASINs.")
    extended_data_creation_date: dt.datetime | None = Field(default=None, description="When the ad was created.")
    extended_data_last_update_date: dt.datetime | None = Field(
        default=None, description="When the ad was last changed."
    )
    extended_data_serving_status: str | None = Field(default=None, description="Serving status.")
    extended_data_serving_status_details: str | None = Field(
        default=None, description="JSON array of the serving status details."
    )
