import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class LineItemsStats(Schema):
    """DBM line items report with performance metrics including impressions, clicks, costs, and fees."""

    advertiser_currency: str | None = Field(..., description="The advertiser currency")
    advertiser_name: str | None = Field(..., description="The advertiser name")
    advertiser: str | None = Field(..., description="The advertiser ID")
    budget_segment_description: str | None = Field(..., description="The budget segment description")
    campaign_manager_360_placement_id: str | None = Field(..., description="The CM360 placement ID")
    creative_id: str | None = Field(..., description="The creative ID")
    creative_type: str | None = Field(..., description="The creative type")
    creative: str | None = Field(..., description="The creative name")
    date: dt.date | None = Field(..., description="The report date")
    insertion_order_name: str | None = Field(..., description="The insertion order name")
    insertion_order: str | None = Field(..., description="The insertion order ID")
    inventory_commitment_type: str | None = Field(..., description="The inventory commitment type")
    inventory_delivery_method: str | None = Field(..., description="The inventory delivery method")
    line_item_end_date: str | None = Field(..., description="The line item end date")
    line_item_name: str | None = Field(..., description="The line item name")
    line_item_start_date: str | None = Field(..., description="The line item start date")
    line_item_type: str | None = Field(..., description="The line item type")
    line_item: str | None = Field(..., description="The line item ID")
    media_plan_name: str | None = Field(..., description="The media plan name")
    media_plan: str | None = Field(..., description="The media plan ID")
    partner_currency: str | None = Field(..., description="The partner currency")
    partner_name: str | None = Field(..., description="The partner name")
    partner: str | None = Field(..., description="The partner ID")
    active_view_average_viewable_time: float | None = Field(
        ..., description="Average viewable time for active view"
    )
    active_view_measurable_impressions: int | None = Field(
        ..., description="Measurable impressions for active view"
    )
    active_view_pct_viewable_impressions: float | None = Field(
        ..., description="Percentage of viewable impressions for active view"
    )
    active_view_percent_visible_on_complete: float | None = Field(
        ..., description="Percent visible on complete for active view"
    )
    active_view_viewable_impressions: int | None = Field(
        ..., description="Viewable impressions for active view"
    )
    billable_impressions: int | None = Field(..., description="The number of billable impressions")
    clicks: int | None = Field(..., description="The number of clicks")
    cpm_fee1_partner: float | None = Field(..., description="CPM fee 1 partner")
    cpm_fee2_partner: float | None = Field(..., description="CPM fee 2 partner")
    ctr: float | None = Field(..., description="Click-through rate")
    data_cost_advertiser: float | None = Field(..., description="Data cost advertiser")
    data_cost_partner: float | None = Field(..., description="Data cost partner")
    fee32_advertiser: float | None = Field(..., description="Fee 32 advertiser")
    fee32_partner: float | None = Field(..., description="Fee 32 partner")
    impressions: int | None = Field(..., description="The number of impressions")
    last_clicks: int | None = Field(..., description="Last clicks")
    last_impressions: int | None = Field(..., description="Last impressions")
    media_cost_advertiser: float | None = Field(..., description="Media cost advertiser")
    media_cost_partner: float | None = Field(..., description="Media cost partner")
    media_fee1_partner: float | None = Field(..., description="Media fee 1 partner")
    media_fee2_partner: float | None = Field(..., description="Media fee 2 partner")
    revenue_advertiser: float | None = Field(..., description="Revenue advertiser")
    total_conversions: int | None = Field(..., description="Total conversions")
    total_media_cost_advertiser: float | None = Field(..., description="Total media cost advertiser")
    total_media_cost_partner: float | None = Field(..., description="Total media cost partner")
    trueview_cpv_advertiser: float | None = Field(..., description="TrueView CPV advertiser")
    app_mediation_partner_fee_advertiser_currency: float | None = Field(
        ..., description="App mediation partner fee in advertiser currency"
    )
    app_mediation_partner_fee_partner_currency: float | None = Field(
        ..., description="App mediation partner fee in partner currency"
    )
    adlingo_fee_advertiser_currency: float | None = Field(
        ..., description="Adlingo fee in advertiser currency"
    )
    adlingo_fee_partner_currency: float | None = Field(
        ..., description="Adlingo fee in partner currency"
    )
    billable_cost_advertiser: float | None = Field(..., description="Billable cost advertiser")
    billable_cost_partner: float | None = Field(..., description="Billable cost partner")
    fee10_advertiser: float | None = Field(..., description="Fee 10 advertiser")
    fee10_partner: float | None = Field(..., description="Fee 10 partner")
    fee11_advertiser: float | None = Field(..., description="Fee 11 advertiser")
    fee11_partner: float | None = Field(..., description="Fee 11 partner")
    fee12_advertiser: float | None = Field(..., description="Fee 12 advertiser")
    fee12_partner: float | None = Field(..., description="Fee 12 partner")
    fee13_advertiser: float | None = Field(..., description="Fee 13 advertiser")
    fee13_partner: float | None = Field(..., description="Fee 13 partner")
    fee14_advertiser: float | None = Field(..., description="Fee 14 advertiser")
    fee14_partner: float | None = Field(..., description="Fee 14 partner")
    fee15_advertiser: float | None = Field(..., description="Fee 15 advertiser")
    fee15_partner: float | None = Field(..., description="Fee 15 partner")
    fee16_advertiser: float | None = Field(..., description="Fee 16 advertiser")
    fee16_partner: float | None = Field(..., description="Fee 16 partner")
    fee17_advertiser: float | None = Field(..., description="Fee 17 advertiser")
    fee17_partner: float | None = Field(..., description="Fee 17 partner")
    fee18_advertiser: float | None = Field(..., description="Fee 18 advertiser")
    fee18_partner: float | None = Field(..., description="Fee 18 partner")
    fee2_advertiser: float | None = Field(..., description="Fee 2 advertiser")
    fee2_partner: float | None = Field(..., description="Fee 2 partner")
    fee20_advertiser: float | None = Field(..., description="Fee 20 advertiser")
    fee20_partner: float | None = Field(..., description="Fee 20 partner")
    fee21_advertiser: float | None = Field(..., description="Fee 21 advertiser")
    fee21_partner: float | None = Field(..., description="Fee 21 partner")
    fee22_advertiser: float | None = Field(..., description="Fee 22 advertiser")
    fee22_partner: float | None = Field(..., description="Fee 22 partner")
    fee31_advertiser: float | None = Field(..., description="Fee 31 advertiser")
    fee31_partner: float | None = Field(..., description="Fee 31 partner")
    fee4_advertiser: float | None = Field(..., description="Fee 4 advertiser")
    fee4_partner: float | None = Field(..., description="Fee 4 partner")
    fee5_advertiser: float | None = Field(..., description="Fee 5 advertiser")
    fee5_partner: float | None = Field(..., description="Fee 5 partner")
    fee6_advertiser: float | None = Field(..., description="Fee 6 advertiser")
    fee6_partner: float | None = Field(..., description="Fee 6 partner")
    fee7_advertiser: float | None = Field(..., description="Fee 7 advertiser")
    fee7_partner: float | None = Field(..., description="Fee 7 partner")
    fee8_advertiser: float | None = Field(..., description="Fee 8 advertiser")
    fee8_partner: float | None = Field(..., description="Fee 8 partner")
    fee9_advertiser: float | None = Field(..., description="Fee 9 advertiser")
    fee9_partner: float | None = Field(..., description="Fee 9 partner")
    platform_fee_advertiser: float | None = Field(..., description="Platform fee advertiser")
    platform_fee_partner: float | None = Field(..., description="Platform fee partner")
    platform_fee_rate: float | None = Field(..., description="Platform fee rate")
    scibids_fee_advertiser_currency: float | None = Field(
        ..., description="Scibids fee in advertiser currency"
    )
    scibids_fee_partner_currency: float | None = Field(
        ..., description="Scibids fee in partner currency"
    )
    seller_id_blocklist_fee_advertiser_currency: float | None = Field(
        ..., description="Seller ID blocklist fee in advertiser currency"
    )
    seller_id_blocklist_fee_partner_currency: float | None = Field(
        ..., description="Seller ID blocklist fee in partner currency"
    )
