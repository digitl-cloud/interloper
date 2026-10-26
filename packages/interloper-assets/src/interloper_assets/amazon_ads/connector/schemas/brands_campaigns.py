import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class BrandsCampaigns(Schema):
    """Sponsored Brands campaign snapshot. One row per campaign and day, as the v4 campaigns list returns it with its extended data. Creation and update times are epoch numbers whose unit Amazon does not document, kept as sent."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    name: str | None = Field(default=None, description="Campaign name.")
    state: str | None = Field(default=None, description="ENABLED, PAUSED, ARCHIVED or PROPOSED.")
    budget: float | None = Field(default=None, description="Budget amount, in the profile's currency.")
    budget_type: str | None = Field(default=None, description="DAILY or LIFETIME.")
    start_date: dt.date | None = Field(default=None, description="Scheduled start, in the profile's timezone.")
    end_date: dt.date | None = Field(
        default=None, description="Scheduled end, in the profile's timezone; NULL when open-ended."
    )
    portfolio_id: str | None = Field(default=None, description="Portfolio the campaign belongs to.")
    brand_entity_id: str | None = Field(default=None, description="Brand entity the campaign advertises.")
    cost_type: str | None = Field(default=None, description="CPC, VCPM or FIXED_PRICE.")
    goal: str | None = Field(default=None, description="Campaign goal.")
    kpi: str | None = Field(default=None, description="Key performance indicator of the goal.")
    is_multi_ad_groups_enabled: bool | None = Field(
        default=None, description="Whether the campaign holds several ad groups."
    )
    product_location: str | None = Field(default=None, description="Where the advertised products are sold.")
    targeted_pg_deal_id: str | None = Field(
        default=None, description="Programmatic guaranteed deal the campaign targets."
    )
    smart_default: str | None = Field(default=None, description="JSON array of the smart defaults applied.")
    site_restrictions: str | None = Field(
        default=None, description="JSON array of the sites the campaign is restricted to."
    )
    tags: str | None = Field(default=None, description="JSON map of the advertiser's tags.")
    rule_based_budget_applicable_rule_id: str | None = Field(default=None, description="Budget rule in effect.")
    rule_based_budget_applicable_rule_name: str | None = Field(
        default=None, description="Name of the budget rule in effect."
    )
    rule_based_budget_is_processing: bool | None = Field(
        default=None, description="Whether the budget rule is being applied."
    )
    rule_based_budget_value: float | None = Field(default=None, description="Budget set by the rule.")
    bidding_bid_optimization: bool | None = Field(default=None, description="Whether Amazon optimises bids.")
    bidding_bid_optimization_strategy: str | None = Field(
        default=None, description="Bid optimisation strategy (deprecated by Amazon)."
    )
    bidding_bid_adjustments_by_placement: str | None = Field(
        default=None, description="JSON array of placement bid adjustments."
    )
    bidding_bid_adjustments_by_shopper_segment: str | None = Field(
        default=None, description="JSON array of shopper segment bid adjustments."
    )
    bidding_shopper_cohort_bid_adjustments: str | None = Field(
        default=None, description="JSON array of shopper cohort bid adjustments."
    )
    extended_data_creation_date: int | None = Field(
        default=None, description="When the campaign was created, as an epoch number."
    )
    extended_data_last_update_date: int | None = Field(
        default=None, description="When the campaign was last changed, as an epoch number."
    )
    extended_data_serving_status: str | None = Field(default=None, description="Serving status.")
    extended_data_serving_status_details: str | None = Field(
        default=None, description="JSON array of the serving status details."
    )
