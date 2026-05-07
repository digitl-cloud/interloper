from interloper.schema import Schema
from pydantic import Field


class Reach(Schema):
    """Unique reach metrics including impression reach, viewable impression reach, and total reach by campaign."""

    active_view_measurable_impressions: float | None = Field(description="Number of measurable impressions")
    active_view_viewable_impressions: float | None = Field(description="Number of viewable impressions")
    advertiser: str | None = Field(description="Name of the advertiser")
    advertiser_id: str | None = Field(description="ID of the advertiser")
    campaign: str | None = Field(description="Name of the campaign")
    campaign_end_date: str | None = Field(description="End date of the campaign")
    campaign_id: str | None = Field(description="ID of the campaign")
    campaign_start_date: str | None = Field(description="Start date of the campaign")
    country: str | None = Field(description="Country of the campaign")
    impressions: float | None = Field(description="Number of impressions")
    unique_reach_duplicate_impression_reach: str | None = Field(description="Duplicate impression reach for unique users")
    unique_reach_exclusive_impression_reach: str | None = Field(description="Exclusive impression reach for unique users")
    unique_reach_impression_reach: str | None = Field(description="Impression reach for unique users")
    unique_reach_incremental_impression_reach: str | None = Field(description="Incremental impression reach for unique users")
    unique_reach_incremental_total_reach: str | None = Field(description="Incremental total reach for unique users")
    unique_reach_incremental_viewable_impression_reach: str | None = Field(description="Incremental viewable impression reach for unique users")
    unique_reach_total_reach: str | None = Field(description="Total reach for unique users")
    unique_reach_viewable_impression_reach: str | None = Field(description="Viewable impression reach for unique users")
