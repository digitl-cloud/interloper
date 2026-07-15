from interloper.schema import Schema
from pydantic import Field


class LineItemsStats(Schema):
    """Line-item performance, cost and fee metrics per day (Bid Manager standard report)."""

    active_view_average_viewable_time_seconds: float | None = Field(
        default=None, description="The average viewable time in seconds"
    )
    active_view_measurable_impressions: int | None = Field(
        default=None, description="The number of measurable impressions"
    )
    active_view_pct_viewable_impressions: str | None = Field(
        default=None, description="The percentage of viewable impressions"
    )
    active_view_pct_visible_at_completion: str | None = Field(
        default=None, description="The percentage of visible impressions at completion"
    )
    active_view_viewable_impressions: int | None = Field(default=None, description="The number of viewable impressions")
    ad_lingo_fee_adv_currency: float | None = Field(default=None, description="The ad lingo fee in advertiser currency")
    ad_lingo_fee_partner_currency: float | None = Field(
        default=None, description="The ad lingo fee in partner currency"
    )
    ad_xpose_fee_adv_currency: float | None = Field(default=None, description="The AdXpose fee in advertiser currency")
    ad_xpose_fee_partner_currency: float | None = Field(default=None, description="The AdXpose fee in partner currency")
    adloox_fee_adv_currency: float | None = Field(default=None, description="The Adloox fee in advertiser currency")
    adloox_fee_partner_currency: float | None = Field(default=None, description="The Adloox fee in partner currency")
    adloox_pre_bid_fee_adv_currency: float | None = Field(
        default=None, description="The Adloox pre-bid fee in advertiser currency"
    )
    adloox_pre_bid_fee_partner_currency: float | None = Field(
        default=None, description="The Adloox pre-bid fee in partner currency"
    )
    adsafe_fee_adv_currency: float | None = Field(default=None, description="The AdSafe fee in advertiser currency")
    adsafe_fee_partner_currency: float | None = Field(default=None, description="The AdSafe fee in partner currency")
    advanced_ad_serving_fee_advertiser_currency: float | None = Field(
        default=None, description="The advanced ad serving fee in advertiser currency"
    )
    advanced_ad_serving_fee_partner_currency: float | None = Field(
        default=None, description="The advanced ad serving fee in partner currency"
    )
    advertiser: str | None = Field(default=None, description="The advertiser associated with the line item")
    advertiser_currency: str | None = Field(default=None, description="The currency used by the advertiser")
    advertiser_id: str | None = Field(default=None, description="The ID of the advertiser")
    agency_trading_desk_fee_adv_currency: float | None = Field(
        default=None, description="The agency trading desk fee in advertiser currency"
    )
    agency_trading_desk_fee_partner_currency: float | None = Field(
        default=None, description="The agency trading desk fee in partner currency"
    )
    aggregate_knowledge_fee_adv_currency: float | None = Field(
        default=None, description="The Aggregate Knowledge fee in advertiser currency"
    )
    aggregate_knowledge_fee_partner_currency: float | None = Field(
        default=None, description="The Aggregate Knowledge fee in partner currency"
    )
    app_mediation_partner_fee_advertiser_currency: float | None = Field(
        default=None, description="The app mediation partner fee in advertiser currency"
    )
    app_mediation_partner_fee_partner_currency: float | None = Field(
        default=None, description="The app mediation partner fee in partner currency"
    )
    billable_cost_adv_currency: float | None = Field(
        default=None, description="The billable cost in advertiser currency"
    )
    billable_cost_partner_currency: float | None = Field(
        default=None, description="The billable cost in partner currency"
    )
    billable_impressions: int | None = Field(default=None, description="The number of billable impressions")
    budget_segment_name: str | None = Field(default=None, description="The budget segment name")
    campaign: str | None = Field(default=None, description="The campaign associated with the line item")
    campaign_id: str | None = Field(default=None, description="The ID of the campaign")
    click_rate_ctr: str | None = Field(default=None, description="The click-through rate")
    clicks: float | None = Field(default=None, description="The number of clicks")
    cm_360_placement_id: str | None = Field(default=None, description="The CM 360 placement ID")
    com_score_v_ce_in_double_click_fee_adv_currency: float | None = Field(
        default=None, description="The comScore vCE in DoubleClick fee in advertiser currency"
    )
    com_score_v_ce_in_double_click_fee_partner_currency: float | None = Field(
        default=None, description="The comScore vCE in DoubleClick fee in partner currency"
    )
    cpm_fee_1_partner_currency: float | None = Field(default=None, description="The first CPM fee in partner currency")
    cpm_fee_2_partner_currency: float | None = Field(default=None, description="The second CPM fee in partner currency")
    creative: str | None = Field(default=None, description="The creative")
    creative_id: str | None = Field(default=None, description="The creative ID")
    creative_type: str | None = Field(default=None, description="The creative type of the line item")
    data_fees_adv_currency: float | None = Field(default=None, description="The data fees in advertiser currency")
    data_fees_partner_currency: float | None = Field(default=None, description="The data fees in partner currency")
    data_management_platform_fee_adv_currency: float | None = Field(
        default=None, description="The data management platform fee in advertiser currency"
    )
    data_management_platform_fee_partner_currency: float | None = Field(
        default=None, description="The data management platform fee in partner currency"
    )
    date: str | None = Field(default=None, description="The date")
    double_verify_pre_bid_fee_adv_currency: float | None = Field(
        default=None, description="The DoubleVerify pre-bid fee in advertiser currency"
    )
    double_verify_pre_bid_fee_partner_currency: float | None = Field(
        default=None, description="The DoubleVerify pre-bid fee in partner currency"
    )
    evidon_fee_adv_currency: float | None = Field(default=None, description="The Evidon fee in advertiser currency")
    evidon_fee_partner_currency: float | None = Field(default=None, description="The Evidon fee in partner currency")
    impressions: int | None = Field(default=None, description="The number of impressions")
    insertion_order: str | None = Field(default=None, description="The insertion order associated with the line item")
    insertion_order_id: str | None = Field(default=None, description="The ID of the insertion order")
    integral_ad_science_pre_bid_fee_adv_currency: float | None = Field(
        default=None, description="The Integral Ad Science pre-bid fee in advertiser currency"
    )
    integral_ad_science_pre_bid_fee_partner_currency: float | None = Field(
        default=None, description="The Integral Ad Science pre-bid fee in partner currency"
    )
    integral_ad_science_video_fee_adv_currency: float | None = Field(
        default=None, description="The Integral Ad Science video fee in advertiser currency"
    )
    integral_ad_science_video_fee_partner_currency: float | None = Field(
        default=None, description="The Integral Ad Science video fee in partner currency"
    )
    inventory_commitment: str | None = Field(default=None, description="The inventory commitment of the line item")
    inventory_delivery_method: str | None = Field(default=None, description="The inventory delivery method")
    line_item: str | None = Field(default=None, description="The line item name")
    line_item_end_date: str | None = Field(default=None, description="The end date of the line item")
    line_item_id: str | None = Field(default=None, description="The ID of the line item")
    line_item_start_date: str | None = Field(default=None, description="The start date of the line item")
    line_item_type: str | None = Field(default=None, description="The type of the line item")
    media_cost_advertiser_currency: float | None = Field(
        default=None, description="The media cost in advertiser currency"
    )
    media_cost_data_fee_adv_currency: float | None = Field(
        default=None, description="The media cost data fee in advertiser currency"
    )
    media_cost_data_fee_partner_currency: float | None = Field(
        default=None, description="The media cost data fee in partner currency"
    )
    media_cost_partner_currency: float | None = Field(default=None, description="The media cost in partner currency")
    media_fee_1_partner_currency: float | None = Field(
        default=None, description="The first media fee in partner currency"
    )
    media_fee_2_partner_currency: float | None = Field(
        default=None, description="The second media fee in partner currency"
    )
    moat_video_fee_adv_currency: float | None = Field(
        default=None, description="The Moat video fee in advertiser currency"
    )
    moat_video_fee_partner_currency: float | None = Field(
        default=None, description="The Moat video fee in partner currency"
    )
    nielsen_digital_ad_ratings_fee_adv_currency: float | None = Field(
        default=None, description="The Nielsen Digital Ad Ratings fee in advertiser currency"
    )
    nielsen_digital_ad_ratings_fee_partner_currency: float | None = Field(
        default=None, description="The Nielsen Digital Ad Ratings fee in partner currency"
    )
    partner: str | None = Field(default=None, description="The partner associated with the line item")
    partner_currency: str | None = Field(default=None, description="The currency used by the partner")
    partner_id: str | None = Field(default=None, description="The ID of the partner")
    platform_fee_adv_currency: float | None = Field(default=None, description="The platform fee in advertiser currency")
    platform_fee_partner_currency: float | None = Field(
        default=None, description="The platform fee in partner currency"
    )
    platform_fee_rate: str | None = Field(default=None, description="The platform fee rate")
    post_click_conversions: int | None = Field(default=None, description="The number of post-click conversions")
    post_view_conversions: int | None = Field(default=None, description="The number of post-view conversions")
    regulatory_operating_costs_advertiser_currency: float | None = Field(
        default=None, description="The regulatory operating costs in advertiser currency"
    )
    regulatory_operating_costs_partner_currency: float | None = Field(
        default=None, description="The regulatory operating costs in partner currency"
    )
    revenue_adv_currency: float | None = Field(default=None, description="The revenue in advertiser currency")
    shop_local_fee_adv_currency: float | None = Field(
        default=None, description="The shop local fee in advertiser currency"
    )
    shop_local_fee_partner_currency: float | None = Field(
        default=None, description="The shop local fee in partner currency"
    )
    teracent_fee_adv_currency: float | None = Field(default=None, description="The Teracent fee in advertiser currency")
    teracent_fee_partner_currency: float | None = Field(
        default=None, description="The Teracent fee in partner currency"
    )
    third_party_ad_server_fee_adv_currency: float | None = Field(
        default=None, description="The third-party ad server fee in advertiser currency"
    )
    third_party_ad_server_fee_partner_currency: float | None = Field(
        default=None, description="The third-party ad server fee in partner currency"
    )
    total_conversions: int | None = Field(default=None, description="The total number of conversions")
    total_media_cost_advertiser_currency: float | None = Field(
        default=None, description="The total media cost in advertiser currency"
    )
    total_media_cost_partner_currency: float | None = Field(
        default=None, description="The total media cost in partner currency"
    )
    trust_metrics_fee_adv_currency: float | None = Field(
        default=None, description="The trust metrics fee in advertiser currency"
    )
    trust_metrics_fee_partner_currency: float | None = Field(
        default=None, description="The trust metrics fee in partner currency"
    )
    vizu_fee_adv_currency: float | None = Field(default=None, description="The Vizu fee in advertiser currency")
    vizu_fee_partner_currency: float | None = Field(default=None, description="The Vizu fee in partner currency")
    you_tube_revenue_e_cpv_adv_currency: float | None = Field(
        default=None, description="The YouTube revenue eCPV in advertiser currency"
    )
