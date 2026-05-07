import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class SiteByCountryPage(Schema):
    """Site search analytics segmented by country and page URL."""

    clicks: int | None = Field(description="The number of times users clicked on search results for this site, country, and page")
    country: str | None = Field(description="The country associated with the search results")
    ctr: float | None = Field(description="The click-through rate for this site, country, and page combination")
    date: dt.date | None = Field(description="The date of the search results")
    impressions: int | None = Field(description="The number of times this site, country, and page combination appeared in search results")
    page: str | None = Field(description="The page URL associated with the search results")
    position: float | None = Field(description="The average position in search results")
    search_type: str | None = Field(description="The type of search performed")
    site_url: str | None = Field(description="The URL of the site")
