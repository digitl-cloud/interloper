import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ActionUpdates(Schema):
    """Impact.com action updates tracking changes to conversion actions over time."""

    id: str | None = Field(default=None, description="Action update ID")
    action_id: str | None = Field(default=None, description="Parent action ID")
    campaign_id: str | None = Field(default=None, description="Campaign (program) ID")
    campaign_name: str | None = Field(default=None, description="Campaign (program) name")
    action_tracker_id: str | None = Field(default=None, description="Action tracker ID")
    action_tracker_name: str | None = Field(default=None, description="Action tracker name")
    state: str | None = Field(default=None, description="Updated action state")
    old_state: str | None = Field(default=None, description="Previous action state")
    ad_id: str | None = Field(default=None, description="Ad ID")
    media_partner_id: str | None = Field(default=None, description="Media partner ID")
    media_partner_name: str | None = Field(default=None, description="Media partner name")
    event_date: dt.datetime | None = Field(default=None, description="Date and time of the event")
    update_date: dt.datetime | None = Field(default=None, description="Date and time of the update")
    creation_date: dt.datetime | None = Field(default=None, description="Date and time of creation")
    locking_date: dt.datetime | None = Field(default=None, description="Date and time when locked")
    clearing_date: dt.datetime | None = Field(default=None, description="Date and time when cleared")
    amount: float | None = Field(default=None, description="Sale amount")
    old_amount: float | None = Field(default=None, description="Previous sale amount")
    currency: str | None = Field(default=None, description="Currency code")
    payout: float | None = Field(default=None, description="Payout amount to partner")
    old_payout: float | None = Field(default=None, description="Previous payout amount")
    delta_payout: float | None = Field(default=None, description="Change in payout amount")
    payout_currency: str | None = Field(default=None, description="Payout currency code")
    oid: str | None = Field(default=None, description="Order ID")
    shared_id: str | None = Field(default=None, description="Shared ID for cross-device tracking")
    promo_code: str | None = Field(default=None, description="Promo code used")
    customer_country: str | None = Field(default=None, description="Customer country")
    customer_status: str | None = Field(default=None, description="Customer status")
    uri: str | None = Field(default=None, description="Action update URI")
    date: dt.date | None = Field(default=None, description="Date of the action update")
