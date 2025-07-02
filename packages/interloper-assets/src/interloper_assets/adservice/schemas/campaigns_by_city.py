from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class CampaignsByCity(itlp.AssetSchema):
    """
    The Campaigns by City report provides insights into campaign performance segmented by cities.
    It includes key metrics such as conversion rates, the number of conversions, and user demographics
    such as browser usage, device type, operating system, and geographical coordinates (latitude and longitude).
    """

    agent_id: int = field(metadata={"description": "The ID of the agent"})
    browser: str = field(metadata={"description": "The browser used"})
    camp_id: int = field(metadata={"description": "The ID of the campaign"})
    camp_title: str = field(metadata={"description": "The title of the campaign"})
    city: str = field(metadata={"description": "The city"})
    conversion_pct: int = field(metadata={"description": "The conversion percentage"})
    conversions: int = field(metadata={"description": "The number of conversions"})
    coordinates_lat: float = field(metadata={"description": "The latitude coordinates"})
    coordinates_lng: float = field(metadata={"description": "The longitude coordinates"})
    country: str = field(metadata={"description": "The country"})
    device_model: str = field(metadata={"description": "The device model"})
    device_type: str = field(metadata={"description": "The device type"})
    os: str = field(metadata={"description": "The operating system"})
