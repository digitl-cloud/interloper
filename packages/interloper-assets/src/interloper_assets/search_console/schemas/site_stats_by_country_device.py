import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class SiteStatsByCountryDevice(Schema):
    """The Site by Country Device report provides insights into how users interact with your site across different countries and devices in search results. It includes key metrics such as clicks, impressions, click-through rate (CTR) and average position per day, country, device and search type."""

    date: dt.date | None = Field(default=None, description="The day of the search, in Pacific Time (PT)")
    country: str | None = Field(default=None, description="The country from which the search originated (ISO 3166-1 alpha-3 code)")
    device: str | None = Field(default=None, description="The type of device used for the search (DESKTOP, MOBILE or TABLET)")
    search_type: str | None = Field(default=None, description="The type of search the row was requested for (web, image, video, news, discover or google_news)")
    clicks: int | None = Field(default=None, description="The number of times a user clicked on a search result link to your site")
    impressions: int | None = Field(default=None, description="The number of times a URL from your site appeared in search results")
    ctr: float | None = Field(default=None, description="The click-through rate (CTR), calculated as the number of clicks divided by the number of impressions")
    position: float | None = Field(default=None, description="The average position of your site's URLs in search results")
