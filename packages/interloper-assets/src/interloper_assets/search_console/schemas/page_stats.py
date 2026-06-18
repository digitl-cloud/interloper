import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PageStats(Schema):
    """Page-level search analytics with metrics per page URL, query, country, device, and search type."""

    clicks: int | None = Field(description="The number of times a user clicked on a search result link to your site")
    country: str | None = Field(description="The country from which the search originated")
    ctr: float | None = Field(description="The click-through rate, calculated as clicks divided by impressions")
    date: dt.date | None = Field(description="The date of the search query")
    device: str | None = Field(description="The type of device used for the search (e.g., desktop, mobile)")
    impressions: int | None = Field(description="The number of times a URL from your site appeared in search results")
    page: str | None = Field(description="The URL of the page that appeared in search results")
    position: float | None = Field(description="The average position of the page in search results")
    query: str | None = Field(description="The search query entered by the user")
    search_type: str | None = Field(description="The type of search performed (e.g., web, image, video)")
    site_url: str | None = Field(description="The URL of your site")
