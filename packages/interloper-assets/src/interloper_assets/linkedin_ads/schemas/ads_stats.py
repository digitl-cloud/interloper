import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdsStats(Schema):
    """LinkedIn ad performance by day. One row per sponsored share and campaign per day, with cost, delivery, engagement and conversion metrics."""

    date_range_start: dt.date | None = Field(
        default=None, description="The first day covered by the row (UTC, inclusive)."
    )
    date_range_end: dt.date | None = Field(
        default=None, description="The last day covered by the row (UTC, inclusive)."
    )
    pivot_values: str | None = Field(
        default=None, description="The URNs the row is grouped by: the sponsored share, then the campaign (JSON array)."
    )
    cost_in_local_currency: float | None = Field(default=None, description="Total cost in the ad account's currency.")
    impressions: int | None = Field(
        default=None, description="Impressions for Sponsored Content, sends for Sponsored Messaging."
    )
    clicks: int | None = Field(default=None, description="Chargeable clicks on the ad.")
    external_website_conversions: int | None = Field(
        default=None,
        description="Times users took a desired action on an external website after clicking on or seeing the ad.",
    )
    conversion_value_in_local_currency: float | None = Field(
        default=None, description="Value of the conversions in the ad account's currency, as set by the advertiser."
    )
    cost_per_qualified_lead: float | None = Field(
        default=None, description="Cost per qualified lead, in the ad account's currency."
    )
    likes: int | None = Field(default=None, description="Likes on the ad. Sponsored Content only.")
    comments: int | None = Field(default=None, description="Comments on the ad. Sponsored Content only.")
    shares: int | None = Field(default=None, description="Shares of the ad. Sponsored Content only.")
    total_engagements: int | None = Field(default=None, description="All user interactions with the ad unit.")
    follows: int | None = Field(
        default=None, description="Follows gained from the ad. Sponsored Content and Follower ads only."
    )
    average_dwell_time: float | None = Field(
        default=None, description="Average time in seconds more than half of the ad stayed visible in the viewport."
    )
    full_screen_plays: int | None = Field(default=None, description="Times a video was switched to full-screen mode.")
    card_clicks: int | None = Field(default=None, description="Clicks on the cards of a carousel ad.")
    audience_penetration: float | None = Field(
        default=None, description="Unique members reached divided by the total target audience size."
    )
    approximate_member_reach: int | None = Field(
        default=None, description="Estimated number of unique members with at least one impression."
    )
    opens: int | None = Field(default=None, description="Opens of a Sponsored Messaging ad.")
    company_page_clicks: int | None = Field(default=None, description="Clicks leading to the company page.")
