import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStats(Schema):
    """Search Ads 360 campaign performance metrics per day (GAQL over the campaign resource)."""

    campaign_advertising_channel_type: str | None = Field(
        default=None, description="The primary serving target of the campaign (e.g. SEARCH, DISPLAY)."
    )
    campaign_bidding_strategy_type: str | None = Field(
        default=None, description="The type of bidding strategy used by the campaign."
    )
    campaign_id: str | None = Field(default=None, description="The ID of the campaign.")
    campaign_name: str | None = Field(default=None, description="The name of the campaign.")
    campaign_resource_name: str | None = Field(default=None, description="The resource name of the campaign.")
    campaign_status: str | None = Field(default=None, description="The status of the campaign (e.g. ENABLED, PAUSED).")
    customer_account_type: str | None = Field(
        default=None, description="The engine account type of the customer (e.g. GOOGLE_ADS, MICROSOFT)."
    )
    customer_id: str | None = Field(default=None, description="The ID of the customer.")
    customer_resource_name: str | None = Field(default=None, description="The resource name of the customer.")
    metrics_average_cost: float | None = Field(
        default=None, description="The average amount paid per interaction, in micros of the account currency."
    )
    metrics_average_cpc: float | None = Field(
        default=None, description="The average cost per click, in micros of the account currency."
    )
    metrics_clicks: int | None = Field(default=None, description="The number of clicks.")
    metrics_cost_micros: int | None = Field(
        default=None, description="The sum of cost-per-click and cost-per-thousand-impressions costs, in micros."
    )
    metrics_ctr: float | None = Field(default=None, description="Clicks divided by impressions.")
    metrics_impressions: int | None = Field(default=None, description="The number of impressions.")
    segments_date: dt.date | None = Field(default=None, description="The day the metrics were aggregated for.")
