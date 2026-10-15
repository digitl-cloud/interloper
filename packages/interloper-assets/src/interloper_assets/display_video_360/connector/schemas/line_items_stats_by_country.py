from interloper.schema import Schema
from pydantic import Field


class LineItemsStatsByCountry(Schema):
    """Line-item performance and cost metrics per day and country (Bid Manager standard report)."""

    active_view_average_viewable_time_seconds: float | None = Field(
        default=None, description="The average viewable time in seconds for the line item"
    )
    active_view_measurable_impressions: int | None = Field(
        default=None, description="The number of measurable impressions for the line item"
    )
    active_view_pct_viewable_impressions: str | None = Field(
        default=None, description="The percentage of viewable impressions for the line item"
    )
    active_view_pct_visible_at_completion: str | None = Field(
        default=None, description="The percentage of visible impressions at completion for the line item"
    )
    active_view_viewable_impressions: int | None = Field(
        default=None, description="The number of viewable impressions for the line item"
    )
    advertiser: str | None = Field(default=None, description="The advertiser associated with the line item")
    advertiser_currency: str | None = Field(default=None, description="The currency used by the advertiser")
    advertiser_id: str | None = Field(default=None, description="The ID of the advertiser")
    app_mediation_partner_fee_advertiser_currency: float | None = Field(
        default=None, description="The app mediation partner fee in advertiser currency"
    )
    app_mediation_partner_fee_partner_currency: float | None = Field(
        default=None, description="The app mediation partner fee in partner currency"
    )
    billable_impressions: int | None = Field(
        default=None, description="The number of billable impressions for the line item"
    )
    budget_segment_name: str | None = Field(default=None, description="The name of the budget segment")
    campaign: str | None = Field(default=None, description="The campaign associated with the line item")
    campaign_id: str | None = Field(default=None, description="The ID of the campaign")
    click_rate_ctr: str | None = Field(default=None, description="The click-through rate (CTR) for the line item")
    clicks: float | None = Field(default=None, description="The number of clicks for the line item")
    cm_360_placement_id: str | None = Field(default=None, description="The ID of the CM 360 placement")
    country: str | None = Field(default=None, description="The country associated with the line item")
    cpm_fee_1_partner_currency: float | None = Field(default=None, description="The first CPM fee in partner currency")
    cpm_fee_2_partner_currency: float | None = Field(default=None, description="The second CPM fee in partner currency")
    creative: str | None = Field(default=None, description="The creative associated with the line item")
    creative_id: str | None = Field(default=None, description="The ID of the creative")
    creative_type: str | None = Field(default=None, description="The creative type of the line item")
    data_fees_adv_currency: float | None = Field(default=None, description="The data fees in advertiser currency")
    data_fees_partner_currency: float | None = Field(default=None, description="The data fees in partner currency")
    date: str | None = Field(default=None, description="The date associated with the line item")
    impressions: float | None = Field(default=None, description="The number of impressions for the line item")
    insertion_order: str | None = Field(default=None, description="The insertion order associated with the line item")
    insertion_order_id: str | None = Field(default=None, description="The ID of the insertion order")
    inventory_commitment: str | None = Field(default=None, description="The inventory commitment of the line item")
    inventory_delivery_method: str | None = Field(
        default=None, description="The inventory delivery method of the line item"
    )
    line_item: str | None = Field(default=None, description="The name of the line item")
    line_item_end_date: str | None = Field(default=None, description="The end date of the line item")
    line_item_id: str | None = Field(default=None, description="The ID of the line item")
    line_item_start_date: str | None = Field(default=None, description="The start date of the line item")
    line_item_type: str | None = Field(default=None, description="The type of the line item")
    media_cost_advertiser_currency: float | None = Field(
        default=None, description="The media cost in advertiser currency"
    )
    media_cost_partner_currency: float | None = Field(default=None, description="The media cost in partner currency")
    media_fee_1_partner_currency: float | None = Field(
        default=None, description="The first media fee in partner currency"
    )
    media_fee_2_partner_currency: float | None = Field(
        default=None, description="The second media fee in partner currency"
    )
    partner: str | None = Field(default=None, description="The partner associated with the line item")
    partner_currency: str | None = Field(default=None, description="The currency used by the partner")
    partner_id: str | None = Field(default=None, description="The ID of the partner")
    post_click_conversions: int | None = Field(
        default=None, description="The number of post-click conversions for the line item"
    )
    post_view_conversions: int | None = Field(
        default=None, description="The number of post-view conversions for the line item"
    )
    regulatory_operating_costs_advertiser_currency: float | None = Field(
        default=None, description="The regulatory operating costs in advertiser currency"
    )
    regulatory_operating_costs_partner_currency: float | None = Field(
        default=None, description="The regulatory operating costs in partner currency"
    )
    revenue_adv_currency: float | None = Field(default=None, description="The revenue in advertiser currency")
    total_conversions: int | None = Field(default=None, description="The total number of conversions for the line item")
    total_media_cost_advertiser_currency: float | None = Field(
        default=None, description="The total media cost in advertiser currency"
    )
    total_media_cost_partner_currency: float | None = Field(
        default=None, description="The total media cost in partner currency"
    )
    you_tube_revenue_e_cpv_adv_currency: float | None = Field(
        default=None, description="The YouTube revenue eCPV in advertiser currency"
    )
