import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class SiteByCountryDevice(Schema):
    """Site search analytics segmented by country and device type."""

    clicks: int | None = Field(description="The number of times users clicked on search results for this site, country, and device")
    country: str | None = Field(description="The country associated with the search results")
    ctr: float | None = Field(description="The click-through rate for this site, country, and device combination")
    date: dt.date | None = Field(description="The date of the search results")
    device: str | None = Field(description="The device type associated with the search results")
    impressions: int | None = Field(description="The number of times this site, country, and device combination appeared in search results")
    position: float | None = Field(description="The average position in search results")
    search_type: str | None = Field(description="The type of search performed")
    site_url: str | None = Field(description="The URL of the site")
