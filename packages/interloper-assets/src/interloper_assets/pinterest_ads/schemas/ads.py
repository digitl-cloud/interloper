from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """Pinterest ad entity metadata including creative details and review status."""

    id: int = Field(description="Unique identifier for the ad")
    ad_account_id: int = Field(description="Unique identifier for the ad account")
    ad_group_id: int = Field(description="Unique identifier for the ad group")
    campaign_id: int = Field(description="Unique identifier for the campaign")
    name: str = Field(description="Name of the ad")
    status: str = Field(description="Current status of the ad")
    creative_type: str = Field(description="Type of creative used in the ad")
    pin_id: int = Field(description="Unique identifier for the pin")
    destination_url: str = Field(description="Destination URL for the ad")
    android_deep_link: str = Field(description="Deep link URL for Android devices")
    ios_deep_link: str = Field(description="Deep link URL for iOS devices")
    review_status: str = Field(description="Review status of the ad")
    summary_status: str = Field(description="Summary status of the ad")
    customizable_cta_type: str = Field(description="Type of customizable call-to-action")
    grid_click_type: str = Field(description="Type of click in the grid view")
    is_pin_deleted: bool = Field(description="Indicates if the pin is deleted")
    is_removable: bool = Field(description="Indicates if the ad is removable")
    rejected_reasons: str = Field(description="List of reasons for ad rejection")
    rejection_labels: str = Field(description="Labels associated with ad rejection")
    carousel_destination_urls: str = Field(description="List of destination URLs for carousel ads")
    type: str = Field(description="List of types associated with the ad")
    created_time: int = Field(description="Timestamp when the ad was created")
    updated_time: int = Field(description="Timestamp when the ad was last updated")
