from dataclasses import dataclass, field

import interloper as itlp


@dataclass
class ConversionsReport(itlp.AssetSchema):
    """
    The Conversions report provides insights on conversion events tracked within campaigns.
    It includes key metrics such as the amount of conversions, conversion time, currency, IP geolocation data,
    media information, order details, and user device information such as browser, device, and operating system.
    """

    agent_id: int = field(metadata={"description": "Agent ID"})
    amount: str = field(metadata={"description": "Amount"})
    camp_id: int = field(metadata={"description": "Campaign ID"})
    camp_title: str = field(metadata={"description": "Campaign Title"})
    cart_id: int = field(metadata={"description": "Cart ID"})
    click_ref_id: int = field(metadata={"description": "Click Reference ID"})
    click_stamp: int = field(metadata={"description": "Click Stamp"})
    coid: int = field(metadata={"description": "COID"})
    company_name: str = field(metadata={"description": "Company Name"})
    conversion_time: int = field(metadata={"description": "Conversion Time"})
    currency: str = field(metadata={"description": "Currency"})
    id: int = field(metadata={"description": "ID"})
    ip_city: str = field(metadata={"description": "IP City"})
    ip_country_geoname_id: int = field(metadata={"description": "IP Country Geoname ID"})
    ip_country_iso_code: str = field(metadata={"description": "IP Country ISO Code"})
    ip_country_names_de: str = field(metadata={"description": "IP Country Names (German)"})
    ip_country_names_en: str = field(metadata={"description": "IP Country Names (English)"})
    ip_country_names_es: str = field(metadata={"description": "IP Country Names (Spanish)"})
    ip_country_names_fr: str = field(metadata={"description": "IP Country Names (French)"})
    ip_country_names_ja: str = field(metadata={"description": "IP Country Names (Japanese)"})
    ip_country_names_pt_br: str = field(metadata={"description": "IP Country Names (Portuguese - Brazil)"})
    ip_country_names_ru: str = field(metadata={"description": "IP Country Names (Russian)"})
    ip_country_names_zh_cn: str = field(metadata={"description": "IP Country Names (Chinese - Simplified)"})
    leaddata: str = field(metadata={"description": "Lead Data"})
    media_id: int = field(metadata={"description": "Media ID"})
    media_url: int = field(metadata={"description": "Media URL"})
    medianame: str = field(metadata={"description": "Media Name"})
    order_id: str = field(metadata={"description": "Order ID"})
    price: str = field(metadata={"description": "Price"})
    pricevariable: str = field(metadata={"description": "Price Variable"})
    reject_reason: int = field(metadata={"description": "Reject Reason"})
    result: str = field(metadata={"description": "Result"})
    sensitive_data: int = field(metadata={"description": "Sensitive Data"})
    stamp: str = field(metadata={"description": "Stamp"})
    status: str = field(metadata={"description": "Status"})
    user_agent: str = field(metadata={"description": "User Agent"})
    user_browser: int = field(metadata={"description": "User Browser"})
    user_device: int = field(metadata={"description": "User Device"})
    user_os: int = field(metadata={"description": "User OS"})
