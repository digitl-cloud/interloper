import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CampaignsStats(Schema):
    """Teads campaign performance metrics including delivery, clicks, and completion rates."""

    day: dt.date = Field(description="The date of the record")
    advertiser_id: int = Field(description="The advertiser identifier")
    advertiser_name: str = Field(description="The advertiser name")
    ad_external_integration_code: str = Field(description="External integration code of the ad")
    ad_id: int = Field(description="The ad identifier")
    ad_external_name: str = Field(description="External name of the ad")
    creative_id: int = Field(description="The creative identifier")
    creative_external_name: str = Field(description="External name of the creative")
    creative_external_integration_code: str = Field(description="External integration code of the creative")
    io_line_id: int = Field(description="The IO line identifier")
    io_line_external_name: str = Field(description="External name of the IO line")
    price_advertiser_event: str = Field(description="Advertiser price event type")
    io_line_budget: float = Field(description="IO line budget")
    turnover_value: float = Field(description="Turnover value")
    complete_rate: float = Field(description="Video completion rate")
    budget_delivered_advertising_value: float = Field(description="Budget delivered advertising value")
    budget_delivered_value: float = Field(description="Budget delivered value")
    click: int = Field(description="Number of clicks")
    advertiser_billable_volume: int = Field(description="Advertiser billable volume")
    click_rate: float = Field(description="Click-through rate")
    start: int = Field(description="Number of video starts")
    budget_delivered_average_cpc_value: float = Field(description="Average CPC from delivered budget")
    complete: int = Field(description="Number of video completions")
    budget_delivered_average_cpm_value: float = Field(description="Average CPM from delivered budget")
