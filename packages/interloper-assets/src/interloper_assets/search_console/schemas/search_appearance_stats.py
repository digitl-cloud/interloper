from interloper.schema import Schema
from pydantic import Field


class SearchAppearanceStats(Schema):
    """Search appearance analytics showing how your site appears in different search result types."""

    clicks: int | None = Field(description="The number of times a URL from your site was clicked in search results")
    ctr: float | None = Field(description="The click-through rate, calculated as clicks divided by impressions")
    impressions: int | None = Field(description="The number of times pages from your site were viewed in search results")
    position: float | None = Field(description="The average position of your site's listing in search results")
    search_appearance: str | None = Field(description="The appearance type of your site's listing (e.g., Web, Image, Video)")
    search_type: str | None = Field(description="The type of search performed by the user")
    site_url: str | None = Field(description="The URL of your website")
