import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FollowerStats(Schema):
    """LinkedIn organization follower counts by various dimensions (function, geo, industry, seniority).

    The LinkedIn API returns a single current snapshot (no time dimension); the ``date`` column is
    stamped from the partition value so successive runs accumulate a daily history.
    """

    date: dt.date = Field(description="The date of the record (partition value)")
    follower_counts_by_association_type: str = Field(description="Counts of followers by association type")
    follower_counts_by_function: str = Field(description="Counts of followers by function")
    follower_counts_by_geo: str = Field(description="Counts of followers by geographic location")
    follower_counts_by_geo_country: str = Field(description="Counts of followers by geographic country")
    follower_counts_by_industry: str = Field(description="Counts of followers by industry")
    follower_counts_by_seniority: str = Field(description="Counts of followers by seniority")
    follower_counts_by_staff_count_range: str = Field(description="Counts of followers by staff count range")
    organizational_entity: str = Field(description="The organizational entity URN")
