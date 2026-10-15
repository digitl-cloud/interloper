import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdGroupsStats(Schema):
    """Ad-group performance metrics per day (MyReports Global Standard Adgroup template)."""

    date: dt.date | None = Field(default=None, description="The date of the record")
    partner_id: str | None = Field(default=None, description="Unique identifier for the partner")
    advertiser_id: str | None = Field(default=None, description="Unique identifier for the advertiser")
    campaign_id: str | None = Field(default=None, description="Unique identifier for the campaign")
    ad_group_id: str | None = Field(default=None, description="Unique identifier for the ad group")
    ad_format: str | None = Field(default=None, description="Format of the advertisement")
    creative_id: str | None = Field(default=None, description="Unique identifier for the creative")
    frequency: int | None = Field(default=None, description="Number of times the ad was shown")
    advertiser: str | None = Field(default=None, description="Name of the advertiser")
    campaign: str | None = Field(default=None, description="Name of the campaign")
    ad_group: str | None = Field(default=None, description="Name of the ad group")
    ad_group_budget_in_impressions: int | None = Field(
        default=None, description="Budget of the ad group in impressions"
    )
    ad_group_daily_cap_in_impressions: int | None = Field(
        default=None, description="Daily cap of the ad group in impressions"
    )
    ad_group_daily_target_in_impressions: int | None = Field(
        default=None, description="Daily target of the ad group in impressions"
    )
    ad_group_base_bid_cpm_adv_currency: float | None = Field(
        default=None, description="Base bid CPM in advertiser's currency"
    )
    ad_group_budget_adv_currency: float | None = Field(
        default=None, description="Budget of the ad group in advertiser's currency"
    )
    ad_group_daily_cap_adv_currency: float | None = Field(
        default=None, description="Daily cap of the ad group in advertiser's currency"
    )
    ad_group_daily_target_adv_currency: float | None = Field(
        default=None, description="Daily target of the ad group in advertiser's currency"
    )
    advertiser_currency_code: str | None = Field(default=None, description="Currency code of the advertiser")
    partner_currency_code: str | None = Field(default=None, description="Currency code of the partner")
    creative: str | None = Field(default=None, description="Name of the creative")
    click_conversion_currency_01: float | None = Field(
        default=None, description="Conversion value for click in currency 01"
    )
    view_through_conversion_currency_01: float | None = Field(
        default=None, description="Conversion value for view-through in currency 01"
    )
    click_conversion_currency_02: float | None = Field(
        default=None, description="Conversion value for click in currency 02"
    )
    view_through_conversion_currency_02: float | None = Field(
        default=None, description="Conversion value for view-through in currency 02"
    )
    click_conversion_currency_03: float | None = Field(
        default=None, description="Conversion value for click in currency 03"
    )
    view_through_conversion_currency_03: float | None = Field(
        default=None, description="Conversion value for view-through in currency 03"
    )
    click_conversion_currency_04: float | None = Field(
        default=None, description="Conversion value for click in currency 04"
    )
    view_through_conversion_currency_04: float | None = Field(
        default=None, description="Conversion value for view-through in currency 04"
    )
    click_conversion_currency_05: float | None = Field(
        default=None, description="Conversion value for click in currency 05"
    )
    view_through_conversion_currency_05: float | None = Field(
        default=None, description="Conversion value for view-through in currency 05"
    )
    click_conversion_currency_06: float | None = Field(
        default=None, description="Conversion value for click in currency 06"
    )
    view_through_conversion_currency_06: float | None = Field(
        default=None, description="Conversion value for view-through in currency 06"
    )
    deal_id: str | None = Field(default=None, description="Unique identifier for the deal")
    ad_server_name: str | None = Field(default=None, description="Name of the ad server")
    ad_server_creative_placement_id: str | None = Field(
        default=None, description="Unique identifier for the ad server creative placement"
    )
    bids: int | None = Field(default=None, description="Number of bids")
    total_bid_amount_adv_currency: float | None = Field(
        default=None, description="Total bid amount in advertiser's currency"
    )
    total_bid_amount_partner_currency: float | None = Field(
        default=None, description="Total bid amount in partner's currency"
    )
    total_bid_amount_usd: float | None = Field(default=None, description="Total bid amount in USD")
    impressions: int | None = Field(default=None, description="Number of impressions")
    clicks: int | None = Field(default=None, description="Number of clicks")
    ttd_cost_adv_currency: float | None = Field(default=None, description="TTD cost in advertiser's currency")
    ttd_cost_partner_currency: float | None = Field(default=None, description="TTD cost in partner's currency")
    ttd_cost_usd: float | None = Field(default=None, description="TTD cost in USD")
    partner_cost_adv_currency: float | None = Field(default=None, description="Partner cost in advertiser's currency")
    partner_cost_partner_currency: float | None = Field(default=None, description="Partner cost in partner's currency")
    partner_cost_usd: float | None = Field(default=None, description="Partner cost in USD")
    advertiser_cost_adv_currency: float | None = Field(
        default=None, description="Advertiser cost in advertiser's currency"
    )
    advertiser_cost_partner_currency: float | None = Field(
        default=None, description="Advertiser cost in partner's currency"
    )
    advertiser_cost_usd: float | None = Field(default=None, description="Advertiser cost in USD")
    player_views: int | None = Field(default=None, description="Number of player views")
    player_starts: int | None = Field(default=None, description="Number of player starts")
    player_25pct_complete: int | None = Field(
        default=None, description="Number of times the player reached 25% completion"
    )
    player_50pct_complete: int | None = Field(
        default=None, description="Number of times the player reached 50% completion"
    )
    player_75pct_complete: int | None = Field(
        default=None, description="Number of times the player reached 75% completion"
    )
    player_completed_views: int | None = Field(default=None, description="Number of completed views")
    player_mute: int | None = Field(default=None, description="Indicates if the player is muted")
    player_unmute: int | None = Field(default=None, description="Indicates if the player is unmuted")
    player_pause: int | None = Field(default=None, description="Indicates if the player is paused")
    player_resume: int | None = Field(default=None, description="Indicates if the player is resumed")
    player_full_screen: int | None = Field(default=None, description="Indicates if the player is in full screen mode")
    player_errors: str | None = Field(default=None, description="Details of any errors encountered by the player")
    player_skip: int | None = Field(default=None, description="Indicates if the player skipped content")
    player_engaged_views: int | None = Field(default=None, description="Number of engaged views by the player")
    player_rewind: int | None = Field(default=None, description="Indicates if the player rewound content")
    player_expansion: int | None = Field(default=None, description="Indicates if the player expanded")
    player_collapse: int | None = Field(default=None, description="Indicates if the player collapsed")
    player_invitation_accept: int | None = Field(
        default=None, description="Indicates if the player accepted an invitation"
    )
    player_close: int | None = Field(default=None, description="Indicates if the player was closed")
    sampled_tracked_impressions: int | None = Field(default=None, description="Number of sampled tracked impressions")
    sampled_viewed_impressions: int | None = Field(default=None, description="Number of sampled viewed impressions")
    conversion_touch_01: int | None = Field(default=None, description="Number of first conversion touches")
    conversion_touch_revenue_01: float | None = Field(default=None, description="Revenue from first conversion touches")
    click_conversion_01: int | None = Field(default=None, description="Number of first click conversions")
    click_conversion_revenue_01: float | None = Field(default=None, description="Revenue from first click conversions")
    view_through_conversion_01: int | None = Field(default=None, description="Number of first view-through conversions")
    view_through_conversion_revenue_01: float | None = Field(
        default=None, description="Revenue from first view-through conversions"
    )
    time_weighted_decay_conversion_01: int | None = Field(
        default=None, description="Number of first time-weighted decay conversions"
    )
    time_weighted_decay_conversion_revenue_01: float | None = Field(
        default=None, description="Revenue from first time-weighted decay conversions"
    )
    time_weighted_decay_conversion_02: int | None = Field(
        default=None, description="Number of second time-weighted decay conversions"
    )
    time_weighted_decay_conversion_revenue_02: float | None = Field(
        default=None, description="Revenue from second time-weighted decay conversions"
    )
    view_through_conversion_02: int | None = Field(
        default=None, description="Number of second view-through conversions"
    )
    view_through_conversion_revenue_02: float | None = Field(
        default=None, description="Revenue from second view-through conversions"
    )
    click_conversion_02: int | None = Field(default=None, description="Number of second click conversions")
    click_conversion_revenue_02: float | None = Field(default=None, description="Revenue from second click conversions")
    conversion_touch_02: int | None = Field(default=None, description="Number of second conversion touches")
    conversion_touch_revenue_02: float | None = Field(
        default=None, description="Revenue from second conversion touches"
    )
    conversion_touch_03: int | None = Field(default=None, description="Number of third conversion touches")
    conversion_touch_revenue_03: float | None = Field(default=None, description="Revenue from third conversion touches")
    click_conversion_03: int | None = Field(default=None, description="Number of third click conversions")
    click_conversion_revenue_03: float | None = Field(default=None, description="Revenue from third click conversions")
    view_through_conversion_03: int | None = Field(default=None, description="Number of third view-through conversions")
    view_through_conversion_revenue_03: float | None = Field(
        default=None, description="Revenue from third view-through conversions"
    )
    time_weighted_decay_conversion_03: int | None = Field(
        default=None, description="Number of third time-weighted decay conversions"
    )
    time_weighted_decay_conversion_revenue_03: float | None = Field(
        default=None, description="Revenue from third time-weighted decay conversions"
    )
    time_weighted_decay_conversion_04: int | None = Field(
        default=None, description="Number of fourth time-weighted decay conversions"
    )
    time_weighted_decay_conversion_revenue_04: float | None = Field(
        default=None, description="Revenue from fourth time-weighted decay conversions"
    )
    view_through_conversion_04: int | None = Field(
        default=None, description="Number of fourth view-through conversions"
    )
    view_through_conversion_revenue_04: float | None = Field(
        default=None, description="Revenue from fourth view-through conversions"
    )
    click_conversion_04: int | None = Field(default=None, description="Number of fourth click conversions")
    click_conversion_revenue_04: float | None = Field(default=None, description="Revenue from fourth click conversions")
    conversion_touch_04: int | None = Field(default=None, description="Number of fourth conversion touches")
    conversion_touch_revenue_04: float | None = Field(
        default=None, description="Revenue from fourth conversion touches"
    )
    conversion_touch_05: int | None = Field(default=None, description="Number of fifth conversion touches")
    conversion_touch_revenue_05: float | None = Field(default=None, description="Revenue from fifth conversion touches")
    click_conversion_05: int | None = Field(default=None, description="Number of fifth click conversions")
    click_conversion_revenue_05: float | None = Field(default=None, description="Revenue from fifth click conversions")
    view_through_conversion_05: int | None = Field(default=None, description="Number of fifth view-through conversions")
    view_through_conversion_revenue_05: float | None = Field(
        default=None, description="Revenue from fifth view-through conversions"
    )
    time_weighted_decay_conversion_05: int | None = Field(
        default=None, description="Number of fifth time-weighted decay conversions"
    )
    time_weighted_decay_conversion_revenue_05: float | None = Field(
        default=None, description="Revenue from fifth time-weighted decay conversions"
    )
    time_weighted_decay_conversion_06: int | None = Field(
        default=None, description="Number of sixth time-weighted decay conversions"
    )
    time_weighted_decay_conversion_revenue_06: float | None = Field(
        default=None, description="Revenue from sixth time-weighted decay conversions"
    )
    conversion_touch_06: int | None = Field(default=None, description="Number of sixth conversion touches")
    conversion_touch_revenue_06: float | None = Field(default=None, description="Revenue from sixth conversion touches")
    click_conversion_06: int | None = Field(default=None, description="Number of sixth click conversions")
    click_conversion_revenue_06: float | None = Field(default=None, description="Revenue from sixth click conversions")
    view_through_conversion_06: int | None = Field(default=None, description="Number of sixth view-through conversions")
    view_through_conversion_revenue_06: float | None = Field(
        default=None, description="Revenue from sixth view-through conversions"
    )
