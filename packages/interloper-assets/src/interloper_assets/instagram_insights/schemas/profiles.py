from interloper.schema import Schema
from pydantic import Field


class Profiles(Schema):
    """Instagram account profile metadata including follower, following, and media counts."""

    followers_count: int = Field(description="Number of followers")
    follows_count: int = Field(description="Number of follows (following)")
    id: str = Field(description="ID of the Instagram user")
    media_count: int = Field(description="Number of media items")
