from interloper.schema import Schema
from pydantic import Field


class AdsMetadata(Schema):
    """Metadata for individual ads including status, creative, and tracking details."""

    id: str = Field(description="The ID of the ad")
    updated_at: str = Field(description="Timestamp when the ad was last updated")
    created_at: str = Field(description="Timestamp when the ad was created")
    name: str = Field(description="The name of the ad")
    ad_squad_id: str = Field(description="The ID of the ad squad the ad belongs to")
    creative_id: str = Field(description="The ID of the creative")
    third_party_paid_impression_tracking_urls: str = Field(description="Third party paid impression tracking URLs")
    third_party_on_swipe_tracking_urls: str = Field(description="Third party on swipe tracking URLs")
    status: str = Field(description="The status of the ad")
    type: str = Field(description="The type of the ad")
    render_type: str = Field(description="The render type of the ad")
    review_status: str = Field(description="The review status of the ad")
    delivery_status: str = Field(description="The delivery status of the ad")
    review_status_reasons: str = Field(description="The reasons for the review status")
    container_chain_ids: str = Field(description="The container chain IDs of the ad")
