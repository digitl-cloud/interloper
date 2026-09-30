import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CreativesStats(Schema):
    """The Teads creatives report provides daily delivery per creative, within its campaign and line item. It includes key metrics such as billable events, delivered budget, cost per click (CPC), cost per thousand impressions (CPM), total ad cost, spent budget, clicks, clickthrough rate (CTR), video starts, video completes and video completion rate."""

    day: dt.date | None = Field(default=None, description="The day of the delivery")
    advertiser_id: int | None = Field(default=None, description="The ID of the advertiser")
    advertiser_name: str | None = Field(default=None, description="The name of the advertiser")
    campaign_id: int | None = Field(default=None, description="The ID of the campaign")
    campaign_name: str | None = Field(default=None, description="The name of the campaign")
    campaign_external_integration_code: str | None = Field(default=None, description="The external integration code of the campaign")
    creative_id: int | None = Field(default=None, description="The ID of the creative")
    creative_name: str | None = Field(default=None, description="The name of the creative")
    creative_external_integration_code: str | None = Field(default=None, description="The external integration code of the creative")
    line_id: int | None = Field(default=None, description="The ID of the line item")
    line_item_name: str | None = Field(default=None, description="The name of the line item")
    line_item_billable_event: str | None = Field(default=None, description="The billable event of the line item")
    line_item_budget: float | None = Field(default=None, description="The budget of the line item")
    billable_events: int | None = Field(default=None, description="The number of billable events")
    budget_delivered: float | None = Field(default=None, description="The delivered budget")
    budget_delivered_cpc: float | None = Field(default=None, description="The cost per click of the delivered budget")
    budget_delivered_cpm: float | None = Field(default=None, description="The cost per thousand impressions of the delivered budget")
    budget_delivered_total_ad_cost: float | None = Field(default=None, description="The total ad cost of the delivered budget")
    budget_spent: float | None = Field(default=None, description="The amount of budget spent")
    clicks: int | None = Field(default=None, description="The number of clicks")
    clickthrough_rate: float | None = Field(default=None, description="The clickthrough rate")
    video_starts: int | None = Field(default=None, description="The number of video starts")
    video_completes: int | None = Field(default=None, description="The number of video completes")
    video_completion_rate: float | None = Field(default=None, description="The completion rate of the video")
