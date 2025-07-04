import datetime as dt
from dataclasses import field

import interloper as itlp


class Ads(itlp.AssetSchema):
    """
    The Bing Ads report provides insights into advertising campaigns. It includes key metrics such as clicks,
    conversions, impressions, spend, and various ad attributes like titles, descriptions, and URLs.
    """

    absolute_top_impression_rate_percent: float = field(
        metadata={"description": "The absolute top impression rate percent"}
    )
    account_id: str = field(metadata={"description": "The ID of the account"})
    account_name: str = field(metadata={"description": "The name of the account"})
    account_number: str = field(metadata={"description": "The account number"})
    account_status: str = field(metadata={"description": "The status of the account"})
    ad_description: str = field(metadata={"description": "The description of the ad"})
    ad_description_2: str = field(metadata={"description": "The second description of the ad"})
    ad_distribution: str = field(metadata={"description": "The distribution of the ad"})
    ad_group_id: str = field(metadata={"description": "The ID of the ad group"})
    ad_group_name: str = field(metadata={"description": "The name of the ad group"})
    ad_group_status: str = field(metadata={"description": "The status of the ad group"})
    ad_id: str = field(metadata={"description": "The ID of the ad"})
    ad_labels: str = field(metadata={"description": "The ad labels"})
    ad_status: str = field(metadata={"description": "The status of the ad"})
    ad_title: str = field(metadata={"description": "The title of the ad"})
    ad_type: str = field(metadata={"description": "The type of the ad"})
    all_conversion_rate: float = field(metadata={"description": "The conversion rate for all conversions"})
    all_conversions: int = field(metadata={"description": "All conversions"})
    all_conversions_qualified: float = field(metadata={"description": "The qualified conversions for all conversions"})
    all_cost_per_conversion: float = field(metadata={"description": "The cost per conversion for all conversions"})
    all_return_on_ad_spend: float = field(metadata={"description": "The return on ad spend for all conversions"})
    all_revenue: float = field(metadata={"description": "All revenue"})
    all_revenue_per_conversion: float = field(
        metadata={"description": "The revenue per conversion for all conversions"}
    )
    assists: int = field(metadata={"description": "The number of assists"})
    average_cpc: float = field(metadata={"description": "The average cost per click"})
    average_cpm: float = field(metadata={"description": "The average cost per thousand impressions"})
    average_position: float = field(metadata={"description": "The average position"})
    base_campaign_id: str = field(metadata={"description": "The ID of the base campaign"})
    bid_match_type: str = field(metadata={"description": "The bid match type"})
    business_name: str = field(metadata={"description": "The business name"})
    campaign_id: str = field(metadata={"description": "The ID of the campaign"})
    campaign_name: str = field(metadata={"description": "The name of the campaign"})
    campaign_status: str = field(metadata={"description": "The status of the campaign"})
    campaign_type: str = field(metadata={"description": "The type of the campaign"})
    clicks: int = field(metadata={"description": "The number of clicks"})
    conversion_rate: float = field(metadata={"description": "The conversion rate"})
    conversions: int = field(metadata={"description": "The number of conversions"})
    conversions_qualified: float = field(metadata={"description": "The qualified conversions"})
    cost_per_assist: float = field(metadata={"description": "The cost per assist"})
    cost_per_conversion: float = field(metadata={"description": "The cost per conversion"})
    ctr: float = field(metadata={"description": "The click-through rate"})
    currency_code: str = field(metadata={"description": "The currency code"})
    custom_parameters: str = field(metadata={"description": "The custom parameters"})
    customer_id: str = field(metadata={"description": "The ID of the customer"})
    customer_name: str = field(metadata={"description": "The name of the customer"})
    delivered_match_type: str = field(metadata={"description": "The delivered match type"})
    destination_url: str = field(metadata={"description": "The destination URL"})
    device_os: str = field(metadata={"description": "The device OS"})
    device_type: str = field(metadata={"description": "The device type"})
    display_url: str = field(metadata={"description": "The display URL"})
    final_app_url: str = field(metadata={"description": "The final app URL"})
    final_mobile_url: str = field(metadata={"description": "The final mobile URL"})
    final_url: str = field(metadata={"description": "The final URL"})
    final_url_suffix: str = field(metadata={"description": "The suffix of the final URL"})
    goal: str = field(metadata={"description": "The goal"})
    goal_type: str = field(metadata={"description": "The type of the goal"})
    headline: str = field(metadata={"description": "The headline"})
    impressions: int = field(metadata={"description": "The number of impressions"})
    language: str = field(metadata={"description": "The language"})
    long_headline: str = field(metadata={"description": "The long headline"})
    network: str = field(metadata={"description": "The network"})
    path_1: str = field(metadata={"description": "The first path"})
    path_2: str = field(metadata={"description": "The second path"})
    return_on_ad_spend: float = field(metadata={"description": "The return on ad spend"})
    revenue: float = field(metadata={"description": "The revenue"})
    revenue_per_assist: float = field(metadata={"description": "The revenue per assist"})
    revenue_per_conversion: float = field(metadata={"description": "The revenue per conversion"})
    spend: float = field(metadata={"description": "The total spend"})
    time_period: dt.date = field(metadata={"description": "The time period"})
    title_part_1: str = field(metadata={"description": "The first part of the title"})
    title_part_2: str = field(metadata={"description": "The second part of the title"})
    title_part_3: str = field(metadata={"description": "The third part of the title"})
    top_impression_rate_percent: float = field(metadata={"description": "The top impression rate percent"})
    top_vs_other: str = field(metadata={"description": "The top vs other"})
    tracking_template: str = field(metadata={"description": "The tracking template"})
    view_through_conversions: float = field(metadata={"description": "The view-through conversions"})
    view_through_conversions_qualified: float = field(
        metadata={"description": "The qualified view-through conversions"}
    )
    view_through_revenue: float = field(metadata={"description": "The view-through revenue"})
