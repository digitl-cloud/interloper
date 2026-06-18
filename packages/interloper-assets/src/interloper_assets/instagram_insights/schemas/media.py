from interloper.schema import Schema
from pydantic import Field


class Media(Schema):
    """Instagram media (posts, reels, stories) metadata and engagement counts."""

    caption: str = Field(description="The caption of the media")
    comments_count: int = Field(description="The number of comments on the media")
    id: str = Field(description="The unique identifier of the media")
    ig_id: str = Field(description="The Instagram ID of the media")
    is_comment_enabled: bool = Field(description="Whether comments are enabled on the media")
    is_shared_to_feed: bool = Field(description="Whether the media is shared to the feed")
    like_count: int = Field(description="The number of likes on the media")
    media_product_type: str = Field(description="The product type of the media")
    media_type: str = Field(description="The type of the media")
    media_url: str = Field(description="The URL of the media")
    permalink: str = Field(description="The permalink of the media")
    shortcode: str = Field(description="The shortcode of the media")
    thumbnail_url: str = Field(description="The URL of the thumbnail of the media")
    timestamp: str = Field(description="The timestamp of the media")
    username: str = Field(description="The username associated with the media")
