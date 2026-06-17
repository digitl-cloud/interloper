import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Clicks(Schema):
    """Clicks report detailing various metrics and attributes related to clicks"""

    ad_campaign: str | None = Field(default=None, description="Campaign associated with the advertisement")
    ad_group: str | None = Field(default=None, description="Group associated with the advertisement")
    ad_id: str | None = Field(default=None, description="Unique identifier for the advertisement")
    ad_name: str | None = Field(default=None, description="Name of the advertisement")
    ad_type: str | None = Field(default=None, description="Type of the advertisement")
    browser: str | None = Field(default=None, description="Browser used for the click")
    channel: str | None = Field(default=None, description="Channel through which the click occurred")
    cpc_bid: float | None = Field(default=None, description="Cost per click bid amount")
    customer_area: str | None = Field(default=None, description="Geographical area of the customer")
    customer_city: str | None = Field(default=None, description="City of the customer")
    customer_country: str | None = Field(default=None, description="Country of the customer")
    customer_region: str | None = Field(default=None, description="Region of the customer")
    deal_name: str | None = Field(default=None, description="Name of the deal associated with the click")
    deal_scope: str | None = Field(default=None, description="Scope of the deal associated with the click")
    deal_type: str | None = Field(default=None, description="Type of the deal associated with the click")
    device_family: str | None = Field(default=None, description="Family of the device used for the click")
    device_type: str | None = Field(default=None, description="Type of device used for the click")
    event_date: dt.datetime | None = Field(default=None, description="Date when the click event occurred")
    id: str | None = Field(default=None, description="Unique identifier for the click")
    ip_address: str | None = Field(default=None, description="IP address associated with the click")
    landing_page_url: str | None = Field(default=None, description="URL of the landing page for the click")
    media_id: str | None = Field(default=None, description="Unique identifier for the media")
    media_name: str | None = Field(default=None, description="Name of the media")
    os: str | None = Field(default=None, description="Operating system used for the click")
    product_sku: str | None = Field(default=None, description="SKU of the product related to the click")
    profile_id: str | None = Field(default=None, description="Unique identifier for the profile")
    program_id: str | None = Field(default=None, description="Unique identifier for the program")
    program_name: str | None = Field(default=None, description="Name of the program")
    referring_domain: str | None = Field(default=None, description="Domain from which the click was referred")
    referring_url: str | None = Field(default=None, description="URL from which the click was referred")
    shared_id: str | None = Field(default=None, description="Shared identifier for tracking purposes")
    unique_click: bool | None = Field(default=None, description="Indicates if the click is unique")
    date: dt.date | None = Field(default=None, description="Partition date")
