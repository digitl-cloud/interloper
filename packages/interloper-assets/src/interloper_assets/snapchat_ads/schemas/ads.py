from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """The Ads entity provides insights into the attributes of indivudal ads in a campaign. It includes key metrics such as ad ID, timestamps for creation and last update, ad name, ad squad ID, creative ID, third-party paid impression tracking URLs, third-party on swipe tracking URLs, ad status, ad type, render type, review status, delivery status, reasons for the review status, and container chain IDs."""

    id: str | None = Field(default=None, description="The ID of the ad")
    updated_at: str | None = Field(default=None, description="The timestamp when the ad was last updated")
    created_at: str | None = Field(default=None, description="The timestamp when the ad was created")
    name: str | None = Field(default=None, description="The name of the ad")
    ad_squad_id: str | None = Field(default=None, description="The ID of the ad squad that the ad belongs to")
    creative_id: str | None = Field(default=None, description="The ID of the creative")
    third_party_paid_impression_tracking_urls: str | None = Field(default=None, description="Third party paid impression tracking URLs")
    third_party_on_swipe_tracking_urls: str | None = Field(default=None, description="Third party on swipe tracking URLs")
    status: str | None = Field(default=None, description="The status of the ad")
    type: str | None = Field(default=None, description="The type of the ad")
    render_type: str | None = Field(default=None, description="The render type of the ad")
    review_status: str | None = Field(default=None, description="The review status of the ad")
    delivery_status: str | None = Field(default=None, description="The delivery status of the ad")
    review_status_reasons: str | None = Field(default=None, description="The reasons for the review status of the ad")
    container_chain_ids: str | None = Field(default=None, description="The container chain IDs of the ad")
