import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdGroups(Schema):
    """The partner's ad groups with their campaign, status, channel and budget (template ad groups excluded)."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="The ID of the ad group")
    name: str | None = Field(default=None, description="The name of the ad group")
    description: str | None = Field(default=None, description="The description of the ad group")
    campaign_id: str | None = Field(default=None, description="The ID of the campaign the ad group belongs to")
    is_enabled: bool | None = Field(default=None, description="Whether the ad group is enabled to bid")
    status: str | None = Field(default=None, description="The status of the ad group")
    channel: str | None = Field(default=None, description="The channel the ad group buys")
    funnel_location: str | None = Field(default=None, description="The funnel stage the ad group targets")
    industry_category_id: str | None = Field(default=None, description="The ID of the ad group's industry category")
    is_programmatic_guaranteed: bool | None = Field(
        default=None, description="Whether the ad group buys programmatic guaranteed inventory"
    )
    budget_in_advertiser_currency: float | None = Field(
        default=None, description="The ad group's budget, in the advertiser's currency"
    )
    budget_in_impressions: int | None = Field(default=None, description="The ad group's budget, in impressions")
    created_at: dt.datetime | None = Field(default=None, description="When the ad group was created")
    last_updated: dt.datetime | None = Field(default=None, description="When the ad group was last changed")
