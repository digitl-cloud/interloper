import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """Bing Ads ad snapshot. One row per ad and day, as the Bulk API's ad records carry them; each ad type fills its own columns."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    type: str | None = Field(
        default=None,
        description="Ad type, as the Bulk record names it (Responsive Search Ad, Expanded Text Ad, Product Ad...).",
    )
    status: str | None = Field(default=None, description="Configured status (Active, Paused, Deleted).")
    id: str | None = Field(default=None, description="Ad id.")
    parent_id: str | None = Field(default=None, description="Ad group the ad belongs to.")
    campaign: str | None = Field(default=None, description="Name of the campaign the ad belongs to.")
    ad_group: str | None = Field(default=None, description="Name of the ad group the ad belongs to.")
    client_id: str | None = Field(default=None, description="Free-text label the advertiser gives the record.")
    modified_time: dt.datetime | None = Field(default=None, description="When the ad was last changed, in UTC.")
    verified_tracking_setting: str | None = Field(
        default=None, description="Verified tracking settings of a responsive ad, as JSON."
    )
    title: str | None = Field(default=None, description="Title of a text or app install ad.")
    text: str | None = Field(default=None, description="Ad text.")
    text_part_2: str | None = Field(default=None, description="Second ad text.")
    display_url: str | None = Field(default=None, description="Display URL of a text ad.")
    destination_url: str | None = Field(default=None, description="Destination URL of a text ad.")
    business_name: str | None = Field(default=None, description="Business name of a responsive ad.")
    promotion: str | None = Field(default=None, description="Promotional text of a product ad.")
    editorial_status: str | None = Field(
        default=None, description="Editorial review status (Active, Disapproved, Inactive, ActiveLimited)."
    )
    editorial_appeal_status: str | None = Field(
        default=None, description="Status of an appeal against an editorial disapproval."
    )
    device_preference: str | None = Field(default=None, description="Devices the ad prefers (All, Mobile).")
    app_platform: str | None = Field(default=None, description="Platform of an app install ad's app.")
    app_id: str | None = Field(default=None, description="Store id of an app install ad's app.")
    final_url: str | None = Field(default=None, description="Landing page URLs, semicolon separated.")
    mobile_final_url: str | None = Field(default=None, description="Mobile landing page URLs, semicolon separated.")
    tracking_template: str | None = Field(default=None, description="Tracking template.")
    custom_parameter: str | None = Field(default=None, description="Custom URL parameters, as Bulk serializes them.")
    ad_format_preference: str | None = Field(default=None, description="Ad format preference (All, Native).")
    description: str | None = Field(
        default=None, description="Descriptions of a responsive search ad, as a JSON array of text assets."
    )
    title_part_1: str | None = Field(default=None, description="First title of an expanded text ad.")
    title_part_2: str | None = Field(default=None, description="Second title of an expanded text ad.")
    title_part_3: str | None = Field(default=None, description="Third title of an expanded text ad.")
    path_1: str | None = Field(default=None, description="First display URL path.")
    path_2: str | None = Field(default=None, description="Second display URL path.")
    domain: str | None = Field(default=None, description="Domain of the display URL.")
    call_to_action: str | None = Field(default=None, description="Call to action of a responsive ad.")
    headline: str | None = Field(
        default=None,
        description="Short headline of a responsive ad; the headlines of a responsive search ad, as a JSON array of text assets.",
    )
    images: str | None = Field(default=None, description="Images of a responsive ad, as JSON.")
    long_headline: str | None = Field(default=None, description="Long headline of a responsive ad.")
    long_headlines: str | None = Field(default=None, description="Long headlines of a responsive ad, as JSON.")
    impression_tracking_urls: str | None = Field(
        default=None, description="Impression tracking URLs of a responsive ad."
    )
    headlines: str | None = Field(default=None, description="Headlines of a responsive ad, as JSON.")
    descriptions: str | None = Field(default=None, description="Descriptions of a responsive ad, as JSON.")
    call_to_action_language: str | None = Field(
        default=None, description="Language of a responsive ad's call to action."
    )
    videos: str | None = Field(default=None, description="Videos of a responsive ad, as JSON.")
    final_url_suffix: str | None = Field(default=None, description="Final URL suffix.")
