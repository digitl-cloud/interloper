import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class AdGroups(Schema):
    """Bing Ads ad group snapshot. One row per ad group and day, as the Bulk API's Ad Group record carries it."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    type: str | None = Field(default=None, description="Bulk record type, always Ad Group.")
    status: str | None = Field(default=None, description="Configured status (Active, Paused, Expired, Deleted).")
    id: str | None = Field(default=None, description="Ad group id.")
    parent_id: str | None = Field(default=None, description="Campaign the ad group belongs to.")
    campaign: str | None = Field(default=None, description="Name of the campaign the ad group belongs to.")
    ad_group: str | None = Field(default=None, description="Ad group name.")
    client_id: str | None = Field(default=None, description="Free-text label the advertiser gives the record.")
    modified_time: dt.datetime | None = Field(default=None, description="When the ad group was last changed, in UTC.")
    multi_media_ad_bid_adjustment: int | None = Field(
        default=None, description="Bid adjustment for multimedia ads, in percent."
    )
    use_optimized_targeting: bool | None = Field(default=None, description="Whether optimized targeting is on.")
    start_date: dt.date | None = Field(
        default=None, description="Day the ad group starts serving, as Bing schedules it."
    )
    end_date: dt.date | None = Field(
        default=None, description="Last day the ad group serves, as Bing schedules it; empty when it runs on."
    )
    network_distribution: str | None = Field(
        default=None, description="Networks the ads serve on (OwnedAndOperatedAndSyndicatedSearch...)."
    )
    ad_rotation: str | None = Field(default=None, description="Ad rotation (OptimizeForClicks, RotateAdsEvenly).")
    cpc_bid: float | None = Field(default=None, description="Default cost per click bid, in the account's currency.")
    cpv_bid: float | None = Field(default=None, description="Default cost per view bid, in the account's currency.")
    cpm_bid: float | None = Field(
        default=None, description="Default cost per thousand impressions bid, in the account's currency."
    )
    language: str | None = Field(default=None, description="Language the ad group targets.")
    privacy_status: str | None = Field(
        default=None, description="Whether the ad group's audience targeting meets the privacy threshold."
    )
    ad_group_type: str | None = Field(
        default=None, description="Ad group type (SearchStandard, SearchDynamic, HotelAds...)."
    )
    hotel_ad_group_type: str | None = Field(default=None, description="Hotel ad group type.")
    commission_rate: float | None = Field(default=None, description="Commission rate of a hotel ad group, in percent.")
    percent_cpc_bid: float | None = Field(default=None, description="Percent cost per click bid of a hotel ad group.")
    bid_adjustment: int | None = Field(
        default=None, description="Bid adjustment for the Microsoft Audience Network, in percent."
    )
    bid_option: str | None = Field(default=None, description="Bid option of a hotel ad group.")
    bid_boost_value: float | None = Field(default=None, description="Bid boost of a hotel ad group.")
    maximum_bid: float | None = Field(
        default=None, description="Maximum bid of a hotel ad group, in the account's currency."
    )
    tracking_template: str | None = Field(default=None, description="Tracking template.")
    custom_parameter: str | None = Field(default=None, description="Custom URL parameters, as Bulk serializes them.")
    bid_strategy_type: str | None = Field(default=None, description="Bid strategy the ad group inherits or sets.")
    target_setting: str | None = Field(
        default=None, description="Criteria the ad group bids on only, semicolon separated."
    )
    ad_schedule_use_searcher_time_zone: bool | None = Field(
        default=None, description="Whether ad schedules follow the searcher's time zone."
    )
    final_url_suffix: str | None = Field(default=None, description="Final URL suffix.")
    base_domain: str | None = Field(default=None, description="Base domain of the ad group's ads.")
