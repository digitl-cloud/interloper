import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStats(Schema):
    """Google Ads campaign performance metrics per day (GAQL over the campaign resource)."""

    campaign_advertising_channel_type: str | None = Field(
        default=None, description="The primary serving target of the campaign (e.g. SEARCH, DISPLAY)."
    )
    campaign_bidding_strategy_type: str | None = Field(
        default=None, description="The type of bidding strategy used by the campaign (e.g. MANUAL_CPC, TARGET_CPA)."
    )
    campaign_budget_amount_micros: int | None = Field(
        default=None, description="The campaign budget amount in micros of the account currency."
    )
    campaign_budget_resource_name: str | None = Field(
        default=None, description="The resource name of the campaign budget."
    )
    campaign_end_date_time: dt.datetime | None = Field(
        default=None, description="The last day and time of the campaign, in the account's time zone."
    )
    campaign_id: str | None = Field(default=None, description="The ID of the campaign.")
    campaign_name: str | None = Field(default=None, description="The name of the campaign.")
    campaign_resource_name: str | None = Field(default=None, description="The resource name of the campaign.")
    campaign_start_date_time: dt.datetime | None = Field(
        default=None, description="The first day and time of the campaign, in the account's time zone."
    )
    campaign_status: str | None = Field(default=None, description="The status of the campaign (e.g. ENABLED, PAUSED).")
    customer_descriptive_name: str | None = Field(default=None, description="The descriptive name of the customer.")
    customer_id: str | None = Field(default=None, description="The ID of the customer.")
    customer_resource_name: str | None = Field(default=None, description="The resource name of the customer.")
    metrics_all_conversions: float | None = Field(
        default=None, description="The total number of conversions, including all conversion actions."
    )
    metrics_all_conversions_value: float | None = Field(default=None, description="The value of all conversions.")
    metrics_average_cost: float | None = Field(
        default=None, description="The average amount paid per interaction, in micros of the account currency."
    )
    metrics_average_cpc: float | None = Field(
        default=None, description="The average cost per click, in micros of the account currency."
    )
    metrics_average_cpm: float | None = Field(
        default=None, description="The average cost per thousand impressions, in micros of the account currency."
    )
    metrics_clicks: int | None = Field(default=None, description="The number of clicks.")
    metrics_conversions: float | None = Field(
        default=None, description="The number of conversions counted in the Conversions column."
    )
    metrics_conversions_from_interactions_rate: float | None = Field(
        default=None, description="Conversions from interactions divided by the number of ad interactions."
    )
    metrics_conversions_value: float | None = Field(
        default=None, description="The value of the conversions counted in the Conversions column."
    )
    metrics_cost_micros: int | None = Field(
        default=None, description="The sum of cost-per-click and cost-per-thousand-impressions costs, in micros."
    )
    metrics_cost_per_all_conversions: float | None = Field(
        default=None, description="The cost divided by all conversions, in micros of the account currency."
    )
    metrics_ctr: float | None = Field(default=None, description="Clicks divided by impressions.")
    metrics_impressions: int | None = Field(default=None, description="The number of impressions.")
    metrics_interaction_rate: float | None = Field(
        default=None, description="Interactions divided by the number of times the ad was shown."
    )
    metrics_interactions: int | None = Field(
        default=None, description="The number of interactions (e.g. clicks, video views, calls)."
    )
    metrics_search_absolute_top_impression_share: float | None = Field(
        default=None,
        description="Search impressions in the absolute top position divided by the eligible search impressions.",
    )
    metrics_search_impression_share: float | None = Field(
        default=None, description="Search impressions received divided by the eligible search impressions."
    )
    metrics_trueview_average_cpv: float | None = Field(
        default=None, description="The average cost per TrueView view, in micros of the account currency."
    )
    segments_date: dt.date | None = Field(default=None, description="The day the metrics were aggregated for.")
