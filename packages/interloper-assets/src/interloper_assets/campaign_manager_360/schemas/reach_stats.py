import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ReachStats(Schema):
    """Unique-reach metrics per campaign and country (CM360 REACH report)."""

    date: dt.date | None = Field(
        default=None, description="The day the report was run for (stamped from the partition)."
    )
    active_view_measurable_impressions: float | None = Field(
        default=None, description="Number of measurable impressions"
    )
    active_view_viewable_impressions: float | None = Field(default=None, description="Number of viewable impressions")
    advertiser: str | None = Field(default=None, description="Name of the advertiser")
    advertiser_id: str | None = Field(default=None, description="ID of the advertiser")
    campaign: str | None = Field(default=None, description="Name of the campaign")
    campaign_end_date: str | None = Field(default=None, description="End date of the campaign")
    campaign_id: str | None = Field(default=None, description="ID of the campaign")
    campaign_start_date: str | None = Field(default=None, description="Start date of the campaign")
    country: str | None = Field(default=None, description="Country of the campaign")
    impressions: float | None = Field(default=None, description="Number of impressions")
    unique_reach_duplicate_impression_reach: str | None = Field(
        default=None, description="Duplicate impression reach for unique users"
    )
    unique_reach_exclusive_impression_reach: str | None = Field(
        default=None, description="Exclusive impression reach for unique users"
    )
    unique_reach_impression_reach: str | None = Field(default=None, description="Impression reach for unique users")
    unique_reach_incremental_impression_reach: str | None = Field(
        default=None, description="Incremental impression reach for unique users"
    )
    unique_reach_incremental_total_reach: str | None = Field(
        default=None, description="Incremental total reach for unique users"
    )
    unique_reach_incremental_viewable_impression_reach: str | None = Field(
        default=None, description="Incremental viewable impression reach for unique users"
    )
    unique_reach_total_reach: str | None = Field(default=None, description="Total reach for unique users")
    unique_reach_viewable_impression_reach: str | None = Field(
        default=None, description="Viewable impression reach for unique users"
    )
