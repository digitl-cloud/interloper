from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class CampaignsByDeviceType(itlp.AssetSchema):
    """
    The Campaigns by Device Type report provides insights into campaign performance
    based on the type of device used by users. It includes key metrics such as
    conversion rates, the number of conversions, and user demographics such as
    browser usage, operating system, city, and country.
    """

    agent_id: int = field(metadata={"description": "The ID of the agent"})
    browser: str = field(metadata={"description": "The browser used"})
    camp_id: int = field(metadata={"description": "The ID of the campaign"})
    camp_title: str = field(metadata={"description": "The title of the campaign"})
    city: str = field(metadata={"description": "The city where the campaign was run"})
    conversion_pct: int = field(metadata={"description": "The percentage of conversions"})
    conversions: int = field(metadata={"description": "The number of conversions"})
    country: str = field(metadata={"description": "The country where the campaign was run"})
    device_model: str = field(metadata={"description": "The model of the device"})
    device_type: str = field(metadata={"description": "The type of the device"})
    os: str = field(metadata={"description": "The operating system used"})
