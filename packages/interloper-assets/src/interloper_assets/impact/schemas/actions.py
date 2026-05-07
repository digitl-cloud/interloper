import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Actions(Schema):
    """Impact.com conversion actions including revenue, commissions, and attribution data."""

    id: str | None = Field(default=None, description="Action ID")
    campaign_id: str | None = Field(default=None, description="Campaign (program) ID")
    campaign_name: str | None = Field(default=None, description="Campaign (program) name")
    action_tracker_id: str | None = Field(default=None, description="Action tracker ID")
    action_tracker_name: str | None = Field(default=None, description="Action tracker name")
    state: str | None = Field(default=None, description="Action state (PENDING, APPROVED, REVERSED)")
    ad_id: str | None = Field(default=None, description="Ad ID")
    caller: str | None = Field(default=None, description="Caller identifier")
    media_partner_id: str | None = Field(default=None, description="Media partner ID")
    media_partner_name: str | None = Field(default=None, description="Media partner name")
    event_date: dt.datetime | None = Field(default=None, description="Date and time of the event")
    creation_date: dt.datetime | None = Field(default=None, description="Date and time of creation")
    locking_date: dt.datetime | None = Field(default=None, description="Date and time when the action was locked")
    clearing_date: dt.datetime | None = Field(default=None, description="Date and time when the action was cleared")
    amount: float | None = Field(default=None, description="Sale amount")
    currency: str | None = Field(default=None, description="Currency code")
    payout: float | None = Field(default=None, description="Payout amount to partner")
    delta_payout: float | None = Field(default=None, description="Change in payout amount")
    intended_payout: float | None = Field(default=None, description="Originally intended payout amount")
    payout_currency: str | None = Field(default=None, description="Payout currency code")
    customer_city: str | None = Field(default=None, description="Customer city")
    customer_region: str | None = Field(default=None, description="Customer region/state")
    customer_country: str | None = Field(default=None, description="Customer country")
    customer_status: str | None = Field(default=None, description="Customer status (new/existing)")
    oid: str | None = Field(default=None, description="Order ID")
    shared_id: str | None = Field(default=None, description="Shared ID for cross-device tracking")
    sub_id_1: str | None = Field(default=None, description="Sub ID 1 (custom tracking parameter)")
    sub_id_2: str | None = Field(default=None, description="Sub ID 2 (custom tracking parameter)")
    sub_id_3: str | None = Field(default=None, description="Sub ID 3 (custom tracking parameter)")
    promo_code: str | None = Field(default=None, description="Promo code used")
    referring_domain: str | None = Field(default=None, description="Referring domain")
    landing_page_url: str | None = Field(default=None, description="Landing page URL")
    ip_address: str | None = Field(default=None, description="IP address")
    uri: str | None = Field(default=None, description="Action URI")
    date: dt.date | None = Field(default=None, description="Date of the action")
