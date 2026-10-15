import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """Pinterest ad snapshots with their creative, destination, review status and tracking settings."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    ad_account_id: str | None = Field(default=None, description="The ID of the advertiser that this ad belongs to.")
    ad_group_id: str | None = Field(default=None, description="ID of the ad group that contains the ad.")
    android_deep_link: str | None = Field(default=None, description="Deep link URL for Android devices.")
    campaign_id: str | None = Field(default=None, description="ID of the ad campaign that contains this ad.")
    carousel_android_deep_links: str | None = Field(
        default=None, description="Comma-separated deep links for the carousel pin on Android."
    )
    carousel_destination_urls: str | None = Field(
        default=None, description="Comma-separated destination URLs for the carousel pin to promote."
    )
    carousel_ios_deep_links: str | None = Field(
        default=None, description="Comma-separated deep links for the carousel pin on iOS."
    )
    carting_platform_type: int | None = Field(
        default=None, description="The vendor platform type of the carting/WTB ad."
    )
    carting_products: str | None = Field(default=None, description="Array of carting/WTB products for the ad.")
    click_tracking_url: str | None = Field(default=None, description="Tracking url for the ad clicks.")
    collection_items_destination_url_template: str | None = Field(
        default=None, description="Destination URL template for all items within a collections drawer."
    )
    collections_header_type: str | None = Field(default=None, description="Collections ad header type for ads")
    created_time: dt.datetime | None = Field(default=None, description="Pin creation time.")
    creative_type: str | None = Field(default=None, description="Ad creative type enum.")
    customizable_cta_type: str | None = Field(
        default=None, description="Select a call to action (CTA) to display below your ad."
    )
    destination_url: str | None = Field(default=None, description="Destination URL.")
    disclosure_type: str | None = Field(
        default=None,
        description="Type of information in the page referenced by `disclosure_url`, provided either by the Food and Drug Administration (FDA) or the manufacturer.",
    )
    disclosure_url: str | None = Field(
        default=None,
        description="URL for a page that provides disclosures about a pharmaceutical product, such as potential side effects.",
    )
    grid_click_type: str | None = Field(
        default=None, description="Where a user is taken after clicking on an ad in grid."
    )
    id: str | None = Field(default=None, description="The ID of this ad.")
    ios_deep_link: str | None = Field(default=None, description="Deep link URL for iOS devices.")
    is_carting: bool | None = Field(default=None, description="Is the ad a carting/WTB ad?")
    is_collage_accepted_terms: bool | None = Field(
        default=None, description="Whether the advertiser has accepted the terms and conditions for collage ad."
    )
    is_collage_single_destination: bool | None = Field(
        default=None, description="Whether the collage ad has a single destination url override."
    )
    is_pin_deleted: bool | None = Field(default=None, description="Is original pin deleted?")
    is_removable: bool | None = Field(default=None, description="Is pin repinnable?")
    lead_form_id: str | None = Field(default=None, description="Lead form ID for lead ad generation.")
    name: str | None = Field(default=None, description="Name of the ad - 255 chars max.")
    pin_id: str | None = Field(default=None, description="Pin ID.")
    quiz_pin_data_questions: str | None = Field(default=None, description="The quiz ad's questions.")
    quiz_pin_data_results: str | None = Field(default=None, description="The quiz ad's results.")
    quiz_pin_data_tie_breaker_custom_result: str | None = Field(
        default=None, description="The result, and link out, based on the user\u2019s choice."
    )
    quiz_pin_data_tie_breaker_type: str | None = Field(
        default=None, description="Quiz ad tie breaker type, default is RANDOM"
    )
    rejected_reasons: str | None = Field(default=None, description="Enum reason why the pin was rejected.")
    rejection_labels: str | None = Field(default=None, description="Text reason why the pin was rejected.")
    review_status: str | None = Field(default=None, description="Ad review status")
    status: str | None = Field(default=None, description="Entity status")
    summary_status: str | None = Field(default=None, description="Ad summary status")
    tracking_urls_audience_verification: str | None = Field(
        default=None, description="Third-party tracking URLs (audience_verification)."
    )
    tracking_urls_buyable_button: str | None = Field(
        default=None, description="Third-party tracking URLs (buyable_button)."
    )
    tracking_urls_click: str | None = Field(default=None, description="Third-party tracking URLs (click).")
    tracking_urls_engagement: str | None = Field(default=None, description="Third-party tracking URLs (engagement).")
    tracking_urls_impression: str | None = Field(default=None, description="Third-party tracking URLs (impression).")
    type: str | None = Field(default=None, description='Always "ad".')
    updated_time: dt.datetime | None = Field(default=None, description="Last update time.")
    view_tracking_url: str | None = Field(default=None, description="Tracking URL for ad impressions.")
