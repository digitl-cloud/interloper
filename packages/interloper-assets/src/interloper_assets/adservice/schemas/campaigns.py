import datetime as dt
from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class Campaigns(itlp.AssetSchema):
    """
    The Campaigns report provides insights into the performance and financial outcomes of advertising campaigns.
    It includes key metrics such as click rates, conversion rates, revenue from various client activities such as
    clicks, impressions, leads, and sales, as well as campaign and media information.
    """

    agent_id: int = field(metadata={"description": "The unique identifier for the agent"})
    camp_id: int = field(metadata={"description": "The unique identifier for the campaign"})
    camp_title: str = field(metadata={"description": "The title of the campaign"})
    campaign_manager: str = field(metadata={"description": "The name of the campaign manager"})
    campaign_manager_id: int = field(metadata={"description": "The unique identifier for the campaign manager"})
    click_nun: int = field(metadata={"description": "The number of clicks from unique visitors"})
    click_un: int = field(metadata={"description": "The number of clicks from unique sources"})
    client_click_rev: float = field(metadata={"description": "Revenue generated from client clicks"})
    client_impr_rev: float = field(metadata={"description": "Revenue generated from client impressions"})
    client_lead_price: float = field(metadata={"description": "Price of leads for the client"})
    client_lead_rev: float = field(metadata={"description": "Revenue generated from client leads"})
    client_sale_rev: float = field(metadata={"description": "Revenue generated from client sales"})
    client_subtotal: float = field(metadata={"description": "Total revenue generated from client activities"})
    company_name: str = field(metadata={"description": "The name of the company"})
    cpc: float = field(metadata={"description": "Cost per click"})
    cr: float = field(metadata={"description": "Conversion rate"})
    currency: str = field(metadata={"description": "The currency used for transactions"})
    currency_code: str = field(metadata={"description": "The currency code used"})
    currency_id: int = field(metadata={"description": "The unique identifier for the currency"})
    currency_rate: float = field(metadata={"description": "The exchange rate of the currency"})
    date: dt.date = field(metadata={"description": "The date of the record"})
    date_from: dt.date = field(metadata={"description": "The starting date for a time period"})
    date_max: dt.date = field(metadata={"description": "The maximum date in a time period"})
    date_min: dt.date = field(metadata={"description": "The minimum date in a time period"})
    date_to: dt.date = field(metadata={"description": "The ending date for a time period"})
    default_banner: str = field(metadata={"description": "The default banner for the campaign"})
    from_date: dt.date = field(metadata={"description": "The starting date of an event or activity"})
    leads: int = field(metadata={"description": "The number of leads generated"})
    media_id: int = field(metadata={"description": "The unique identifier for the media"})
    medianame: str = field(metadata={"description": "The name of the media"})
    mediatype: str = field(metadata={"description": "The type of media"})
    monthyear: str = field(metadata={"description": "The combination of month and year"})
    pcr: float = field(metadata={"description": "Partial conversion rate"})
    pending_rev: float = field(metadata={"description": "Revenue that is pending or not yet realized"})
    primary_category: int = field(metadata={"description": "The primary category identifier"})
    primary_category_name: str = field(metadata={"description": "The name of the primary category"})
    publisher_manager: str = field(metadata={"description": "The name of the publisher manager"})
    publisher_manager_id: int = field(metadata={"description": "The unique identifier for the publisher manager"})
    sales: float = field(metadata={"description": "The number of sales"})
    sales_amount: float = field(metadata={"description": "The total amount of sales"})
    stamp: dt.date = field(metadata={"description": "The timestamp of the record"})
    to_date: dt.date = field(metadata={"description": "The ending date of an event or activity"})
    week_to: dt.date = field(metadata={"description": "The ending week of a time period"})
    weekyear: str = field(metadata={"description": "The combination of week and year"})
    year: dt.date = field(metadata={"description": "The year of the record"})
    year_from: dt.date = field(metadata={"description": "The starting year of a time period"})
    year_to: dt.date = field(metadata={"description": "The ending year of a time period"})
