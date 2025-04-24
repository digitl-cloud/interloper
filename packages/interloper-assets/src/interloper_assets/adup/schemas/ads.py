import datetime as dt
from dataclasses import field

import interloper as itlp


class Ads(itlp.AssetSchema):
    """
    The AdUp Ads report provides insights into the performance of ads within the AdUp platform.
    It includes key metrics such as impressions, clicks, conversions, average cost, click-through rate (CTR),
    conversion rate, and interaction rate.
    """

    account_descriptive_name: str = field(metadata={"description": "The descriptive name of the account"})
    account_id: int = field(metadata={"description": "The account ID"})
    ad_name: str = field(metadata={"description": "The name of the ad"})
    adgroup_id: int = field(metadata={"description": "The ad group ID"})
    adgroup_name: str = field(metadata={"description": "The name of the ad group"})
    adgroup_status: str = field(metadata={"description": "The status of the ad group"})
    all_conversions: int = field(metadata={"description": "The total number of conversions"})
    average_cost: int = field(metadata={"description": "The average cost"})
    average_cpc: int = field(metadata={"description": "The average cost per click"})
    average_cpm: int = field(metadata={"description": "The average cost per thousand impressions"})
    average_position: float = field(metadata={"description": "The average position"})
    campaign_id: int = field(metadata={"description": "The campaign ID"})
    campaign_name: str = field(metadata={"description": "The name of the campaign"})
    campaign_status: str = field(metadata={"description": "The status of the campaign"})
    clicks: int = field(metadata={"description": "The total number of clicks"})
    conversion_rate: float = field(metadata={"description": "The conversion rate"})
    conversions: int = field(metadata={"description": "The total number of conversions"})
    cost: int = field(metadata={"description": "The total cost"})
    cost_per_all_conversion: int = field(metadata={"description": "The cost per all conversion"})
    cost_per_conversion: int = field(metadata={"description": "The cost per conversion"})
    creative_final_mobile_url: str = field(metadata={"description": "The final mobile URL of the creative"})
    creative_final_url: str = field(metadata={"description": "The final URL of the creative"})
    creative_final_url_suffix: str = field(metadata={"description": "The final URL suffix of the creative"})
    ctr: float = field(metadata={"description": "The click-through rate"})
    date: dt.date = field(metadata={"description": "The date of the data"})
    description: str = field(metadata={"description": "The description"})
    display_url: str = field(metadata={"description": "The display URL"})
    headline: str = field(metadata={"description": "The headline"})
    id: int = field(metadata={"description": "The ID"})
    impressions: int = field(metadata={"description": "The total number of impressions"})
    interaction_rate: float = field(metadata={"description": "The interaction rate"})
    interactions: int = field(metadata={"description": "The total number of interactions"})
    path1: str = field(metadata={"description": "The first path"})
    path2: str = field(metadata={"description": "The second path"})
    view_through_conversions: int = field(metadata={"description": "The total number of view-through conversions"})
