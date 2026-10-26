import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ProductsCampaigns(Schema):
    """Sponsored Products campaign snapshot. One row per campaign and day, as the v3 campaigns list returns it with its extended data. campaign_id is the join key to the Sponsored Products reports."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    name: str | None = Field(default=None, description="Campaign name.")
    state: str | None = Field(
        default=None, description="ENABLED, PAUSED, ARCHIVED, ENABLING, USER_DELETED, PROPOSED or OTHER."
    )
    targeting_type: str | None = Field(default=None, description="AUTO or MANUAL.")
    start_date: dt.date | None = Field(default=None, description="Scheduled start, in the profile's timezone.")
    end_date: dt.date | None = Field(
        default=None, description="Scheduled end, in the profile's timezone; NULL when open-ended."
    )
    portfolio_id: str | None = Field(default=None, description="Portfolio the campaign belongs to.")
    global_campaign_id: str | None = Field(default=None, description="Id of the campaign across marketplaces.")
    auto_manage_campaign: bool | None = Field(
        default=None, description="Whether Amazon manages the campaign automatically."
    )
    marketplace_budget_allocation: str | None = Field(
        default=None, description="AUTO or MANUAL budget allocation across marketplaces."
    )
    site_restrictions: str | None = Field(
        default=None, description="JSON array of the sites the campaign is restricted to."
    )
    tags: str | None = Field(default=None, description="JSON map of the advertiser's tags.")
    budget_budget: float | None = Field(default=None, description="Budget amount, in the profile's currency.")
    budget_budget_type: str | None = Field(default=None, description="DAILY or OTHER.")
    budget_effective_budget: float | None = Field(default=None, description="Budget in effect after budget rules.")
    dynamic_bidding_strategy: str | None = Field(
        default=None, description="LEGACY_FOR_SALES, AUTO_FOR_SALES, MANUAL, RULE_BASED or OTHER."
    )
    dynamic_bidding_placement_bidding: str | None = Field(
        default=None, description="JSON array of placement bid adjustments."
    )
    dynamic_bidding_shopper_cohort_bidding: str | None = Field(
        default=None, description="JSON array of shopper cohort bid adjustments."
    )
    off_amazon_settings_off_amazon_budget_control_strategy: str | None = Field(
        default=None, description="Budget control strategy off Amazon."
    )
    extended_data_creation_date_time: dt.datetime | None = Field(
        default=None, description="When the campaign was created."
    )
    extended_data_last_update_date_time: dt.datetime | None = Field(
        default=None, description="When the campaign was last changed."
    )
    extended_data_serving_status: str | None = Field(
        default=None, description="Serving status, such as CAMPAIGN_STATUS_ENABLED."
    )
    extended_data_serving_status_details: str | None = Field(
        default=None, description="JSON array of the serving status details."
    )
