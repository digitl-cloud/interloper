import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class SearchAppearanceStats(Schema):
    """The Search Appearance report provides insights into how your website appears in Google Search results and user interactions. It includes key metrics such as clicks, impressions, click-through rate (CTR) and average position per search result feature (e.g. AMP, video, rich results) and search type, for the day the report was requested."""

    search_appearance: str | None = Field(default=None, description="The search result feature your site appeared with (e.g. rich results, videos)")
    search_type: str | None = Field(default=None, description="The type of search the row was requested for (web, image, video, news, discover or google_news)")
    clicks: int | None = Field(default=None, description="The number of times a user clicked on a search result link to your site")
    impressions: int | None = Field(default=None, description="The number of times a URL from your site appeared in search results")
    ctr: float | None = Field(default=None, description="The click-through rate (CTR), calculated as the number of clicks divided by the number of impressions")
    position: float | None = Field(default=None, description="The average position of your site's URLs in search results")
    date: dt.date | None = Field(default=None, description="The day the report was requested for, in Pacific Time (PT)")
