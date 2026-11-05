import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Campaigns(Schema):
    """The partner's campaigns with their advertiser, status, schedule, budget, pacing and goals."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="The ID of the campaign")
    name: str | None = Field(default=None, description="The name of the campaign")
    description: str | None = Field(default=None, description="The description of the campaign")
    advertiser_id: str | None = Field(default=None, description="The ID of the advertiser the campaign belongs to")
    status: str | None = Field(default=None, description="The status of the campaign")
    is_archived: bool | None = Field(default=None, description="Whether the campaign is archived")
    created_at_utc: dt.datetime | None = Field(default=None, description="When the campaign was created, in UTC")
    start_date: dt.datetime | None = Field(default=None, description="When the campaign starts spending, a UTC instant")
    end_date: dt.datetime | None = Field(
        default=None, description="When the campaign stops spending, a UTC instant; NULL when open-ended"
    )
    time_zone: str | None = Field(
        default=None, description="The campaign's time zone (IANA name), in which its schedule was set"
    )
    budget_total: float | None = Field(default=None, description="Total budget, in the advertiser's currency")
    budget_total_in_impressions: int | None = Field(default=None, description="Total budget, in impressions")
    impressions_only_budgeting_cpm: float | None = Field(
        default=None, description="CPM used to convert an impressions budget, in the advertiser's currency"
    )
    minimum_ad_group_spend_in_advertiser_currency: float | None = Field(
        default=None, description="Minimum spend per ad group, in the advertiser's currency"
    )
    pacing_mode: str | None = Field(default=None, description="How the campaign paces its budget")
    primary_goal_type: str | None = Field(default=None, description="The type of the campaign's primary goal")
    primary_goal_target: float | None = Field(default=None, description="The target value of the primary goal")
    secondary_goal_type: str | None = Field(default=None, description="The type of the campaign's secondary goal")
    secondary_goal_target: float | None = Field(default=None, description="The target value of the secondary goal")
    tertiary_goal_type: str | None = Field(default=None, description="The type of the campaign's tertiary goal")
    tertiary_goal_target: float | None = Field(default=None, description="The target value of the tertiary goal")
    channel_type: str | None = Field(default=None, description="The campaign's primary channel")
    type: str | None = Field(default=None, description="The campaign's type")
