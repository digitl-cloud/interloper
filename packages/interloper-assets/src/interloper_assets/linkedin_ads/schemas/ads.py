import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Ads(Schema):
    """LinkedIn Ads analytics report with performance, engagement, and conversion metrics."""

    date_range_start: dt.date = Field(description="The start date of the reporting period")
    date_range_end: dt.date = Field(description="The end date of the reporting period")
    pivot_values: str = Field(description="Dimension values used for pivoting the data (e.g., campaign name)")
    # Core Performance & Cost
    cost_in_local_currency: float = Field(description="Total campaign cost in the original local currency")
    impressions: int = Field(description="The total number of times the content was displayed to users")
    clicks: int = Field(description="Total clicks on any part of the ad or content")
    # Conversions & Leads
    external_website_conversions: int = Field(description="Number of conversions that occurred on an external website")
    conversion_value_in_local_currency: float = Field(
        description="The total monetary value of all conversions in local currency"
    )
    cost_per_qualified_lead: float = Field(
        description="The average cost to generate a single lead that meets qualification criteria"
    )
    # Audience Engagement
    likes: int = Field(description="The total number of likes on the content")
    comments: int = Field(description="The total number of comments on the content")
    shares: int = Field(description="The number of shares")
    total_engagements: int = Field(description="The total number of engagements")
    follows: int = Field(description="The number of new follows gained")
    average_dwell_time: float = Field(description="Average time in seconds users spent viewing the content")
    full_screen_plays: int = Field(description="Number of times a video was played in full-screen mode")
    card_clicks: int = Field(description="Number of clicks on a specific card element within the content")
    audience_penetration: float = Field(
        description="The percentage of the target audience reached by the content"
    )
    # Reach & Awareness
    approximate_member_reach: int = Field(
        description="Estimated number of unique members who saw the content"
    )
    opens: int = Field(description="For messaging campaigns, the number of times a message was opened")
    company_page_clicks: int = Field(description="Total clicks on the link leading to the company page")
