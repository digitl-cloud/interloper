import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Media(Schema):
    """Instagram media snapshot. One row per media item of the account per day, with its attributes and like and comment counts."""

    caption: str | None = Field(default=None, description="The caption of the media.")
    comments_count: int | None = Field(default=None, description="The number of comments on the media.")
    id: str | None = Field(default=None, description="The unique identifier of the media.")
    ig_id: str | None = Field(default=None, description="The Instagram ID of the media.")
    is_comment_enabled: bool | None = Field(
        default=None, description="Indicates whether comments are enabled on the media."
    )
    is_shared_to_feed: bool | None = Field(
        default=None, description="Indicates whether the media is shared to the feed."
    )
    like_count: int | None = Field(default=None, description="The number of likes on the media.")
    media_product_type: str | None = Field(default=None, description="The product type of the media.")
    media_type: str | None = Field(default=None, description="The type of the media.")
    media_url: str | None = Field(default=None, description="The URL of the media.")
    permalink: str | None = Field(default=None, description="The permalink of the media.")
    shortcode: str | None = Field(default=None, description="The shortcode of the media.")
    thumbnail_url: str | None = Field(default=None, description="The URL of the thumbnail of the media.")
    timestamp: dt.datetime | None = Field(default=None, description="When the media was published.")
    username: str | None = Field(default=None, description="The username associated with the media.")
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
