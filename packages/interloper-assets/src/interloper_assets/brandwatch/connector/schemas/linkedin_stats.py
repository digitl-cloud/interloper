import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class LinkedinStats(Schema):
    """LinkedIn channel performance metrics per day (Brandwatch/Falcon.io Measure)."""

    date: dt.date | None = Field(default=None, description="The date the metrics correspond to.")
    channel_id: str | None = Field(default=None, description="Unique identifier for the Facebook Page.")
    fans: int | None = Field(default=None, description="Total number of unique followers (Fans) for the page.")
    followers: int | None = Field(
        default=None, description="Total number of people following the page (synonym for fans)."
    )
    net_fans: int | None = Field(default=None, description="Net change in total fans (New - Lost).")
    net_followers: int | None = Field(default=None, description="Net change in total followers.")
    organic_net_fans: int | None = Field(default=None, description="Net change in fans from organic sources.")
    paid_net_fans: int | None = Field(default=None, description="Net change in fans from paid/sponsored sources.")
    fans_by_association_total: int | None = Field(
        default=None, description="Total fans acquired broken down by how they followed the page."
    )
    fans_by_association_employee: int | None = Field(
        default=None, description="Fans acquired who followed the page via an employee's profile/share."
    )
    fans_by_association_non_employee: int | None = Field(
        default=None, description="Fans acquired who followed the page via non-employee sources."
    )
    organic_fans_by_association_total: int | None = Field(
        default=None, description="Organic fans acquired broken down by source."
    )
    organic_fans_by_association_employee: int | None = Field(
        default=None, description="Organic fans acquired via an employee's profile/share."
    )
    organic_fans_by_association_non_employee: int | None = Field(
        default=None, description="Organic fans acquired via non-employee sources."
    )
    paid_fans_by_association_total: int | None = Field(
        default=None, description="Paid fans acquired broken down by source."
    )
    paid_fans_by_association_employee: int | None = Field(
        default=None, description="Paid fans acquired via an employee's profile/share."
    )
    paid_fans_by_association_non_employee: int | None = Field(
        default=None, description="Paid fans acquired via non-employee sources."
    )
    impressions: int | None = Field(default=None, description="Total number of times posts were seen (non-unique).")
    reach: int | None = Field(default=None, description="Total number of unique users who saw the content.")
    frequency: float | None = Field(
        default=None, description="The average number of times a unique user saw the content (Impressions / Reach)."
    )
    engagements: int | None = Field(
        default=None, description="Total engagement actions (Reactions, Comments, Shares, Clicks)."
    )
    interactions: int | None = Field(default=None, description="A general count of interactions.")
    comments: int | None = Field(default=None, description="Total number of comments on posts.")
    shares: int | None = Field(default=None, description="Total number of shares of posts.")
    reactions: int | None = Field(
        default=None, description="Total number of reactions (e.g., Like, Celebrate, Love, Insightful, etc.)."
    )
    clicks: int | None = Field(default=None, description="Total clicks on a post.")
    interaction_rate: float | None = Field(default=None, description="Overall interaction rate.")
    engagement_rate: float | None = Field(default=None, description="Overall engagement rate.")
    interaction_rate_reach: float | None = Field(
        default=None, description="Interaction rate calculated based on reach."
    )
    engagement_rate_reach: float | None = Field(default=None, description="Engagement rate calculated based on reach.")
