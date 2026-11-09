import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdSets(Schema):
    """Criteo ad set snapshot. One row per ad set and day, as the ad set search returns it, nested objects flattened and lists kept as JSON. Criteo schedules the ad set, not the campaign; the schedule's dates are instants, as Criteo sends them. id is the join key to adset_id in the reports."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: str | None = Field(default=None, description="Ad set id.")
    type: str | None = Field(default=None, description="Entity type Criteo names the resource with.")
    name: str | None = Field(default=None, description="Ad set name.")
    advertiser_id: str | None = Field(default=None, description="Advertiser of the ad set's campaign.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad set belongs to.")
    dataset_id: str | None = Field(default=None, description="Dataset of the ad set.")
    destination_environment: str | None = Field(
        default=None, description="Where an ad click leads: undefined, web or app."
    )
    media_type: str | None = Field(default=None, description="Media type of the ad set.")
    objective: str | None = Field(default=None, description="Ad set objective, such as conversions, clicks or reach.")
    video_channel: str | None = Field(default=None, description="Video channel of the ad set.")
    schedule_start_date_value: dt.datetime | None = Field(default=None, description="When the ad set starts serving.")
    schedule_end_date_value: dt.datetime | None = Field(
        default=None, description="When the ad set stops serving; empty when it runs on."
    )
    schedule_activation_status: str | None = Field(
        default=None, description="Whether the advertiser wants the ad set to deliver: on or off."
    )
    schedule_delivery_status: str | None = Field(
        default=None,
        description="Whether the ad set is delivering: draft, inactive, live, notLive, pausing, paused, scheduled, "
        "ended, notDelivering or archived.",
    )
    bidding_bid_amount: float | None = Field(
        default=None, description="Target for the ad set's objective; empty for objectives that need none."
    )
    bidding_cost_controller: str | None = Field(
        default=None, description="Cost controller, such as COS, maxCPC, CPO or dailyBudget."
    )
    budget_budget_strategy: str | None = Field(
        default=None, description="Whether the ad set's spend is limited: capped or uncapped."
    )
    budget_budget_renewal: str | None = Field(
        default=None,
        description="Period the budget is spent over: daily, weekly, monthly or lifetime; undefined when uncapped.",
    )
    budget_budget_delivery_smoothing: str | None = Field(
        default=None, description="Pacing of a capped budget: accelerated or standard."
    )
    budget_budget_delivery_week: str | None = Field(
        default=None, description="Day range a weekly budget is paced over, such as mondayToSunday."
    )
    budget_budget_amount: float | None = Field(
        default=None, description="Budget per renewal period, in the advertiser's currency; empty when uncapped."
    )
    budget_pacing_behavior: str | None = Field(
        default=None, description="How a standard capped budget is paced: targetFullBudget or targetAverageDailyBudget."
    )
    attribution_configuration_attribution_method: str | None = Field(
        default=None, description="How events are attributed to the ad set, such as criteoAttribution or lastClick."
    )
    attribution_configuration_lookback_window: str | None = Field(
        default=None, description="Lookback window of a postClick or lastClick attribution."
    )
    targeting_delivery_limitations_environments: str | None = Field(
        default=None, description="JSON list of the environments targeted; empty list for no limitation."
    )
    targeting_delivery_limitations_devices: str | None = Field(
        default=None, description="JSON list of the devices targeted; empty list for no limitation."
    )
    targeting_delivery_limitations_operating_systems: str | None = Field(
        default=None, description="JSON list of the operating systems targeted; empty list for no limitation."
    )
    targeting_geo_location_countries_value_operand: str | None = Field(
        default=None, description="Whether the listed countries are included or excluded."
    )
    targeting_geo_location_countries_value_values: str | None = Field(
        default=None, description="JSON list of the countries targeted."
    )
    targeting_geo_location_subdivisions_value_operand: str | None = Field(
        default=None, description="Whether the listed subdivisions are included or excluded."
    )
    targeting_geo_location_subdivisions_value_values: str | None = Field(
        default=None, description="JSON list of the subdivisions targeted."
    )
    targeting_geo_location_zip_codes_value_operand: str | None = Field(
        default=None, description="Whether the listed zip codes are included or excluded."
    )
    targeting_geo_location_zip_codes_value_values: str | None = Field(
        default=None, description="JSON list of the zip codes targeted."
    )
    targeting_frequency_capping_value_frequency: str | None = Field(
        default=None, description="Period impressions are capped over: hourly, daily, lifetime or advanced."
    )
    targeting_frequency_capping_value_maximum_impressions: int | None = Field(
        default=None, description="Maximum impressions per user in that period."
    )
