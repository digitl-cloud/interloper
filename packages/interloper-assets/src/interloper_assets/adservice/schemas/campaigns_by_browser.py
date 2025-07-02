from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class CampaignsByBrowser(itlp.AssetSchema):
    """
    The Campaigns by Browser report provides insights into campaign performance based on the user's browser usage.
    It includes key metrics such as conversion rates, the number of conversions, and user demographics such as
    city, country, device model, device type, and operating system.
    """

    agent_id: int = field(metadata={"description": "The ID of the agent"})
    browser: str = field(metadata={"description": "The browser used by the user"})
    camp_id: int = field(metadata={"description": "The ID of the campaign"})
    camp_title: str = field(metadata={"description": "The title of the campaign"})
    city: str = field(metadata={"description": "The city where the user is located"})
    conversion_pct: int = field(metadata={"description": "The percentage of conversions"})
    conversions: int = field(metadata={"description": "The number of conversions"})
    country: str = field(metadata={"description": "The country where the user is located"})
    device_model: str = field(metadata={"description": "The model of the device used by the user"})
    device_type: str = field(metadata={"description": "The type of device used by the user"})
    os: str = field(metadata={"description": "The operating system used by the user"})
