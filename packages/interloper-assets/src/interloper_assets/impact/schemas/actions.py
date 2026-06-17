import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Actions(Schema):
    """Actions report detailing various metrics and attributes related to marketing actions"""

    action_tracker_id: str | None = Field(default=None, description="Unique identifier for the action tracker")
    action_tracker_name: str | None = Field(default=None, description="Name of the action tracker")
    ad_id: str | None = Field(default=None, description="Unique identifier for the advertisement")
    amount: float | None = Field(default=None, description="Total amount involved in the action")
    caller_id: str | None = Field(default=None, description="Identifier for the caller associated with the action")
    campaign_id: str | None = Field(default=None, description="Unique identifier for the campaign")
    campaign_name: str | None = Field(default=None, description="Name of the campaign")
    cleared_date: dt.datetime | None = Field(default=None, description="Date when the action was cleared")
    client_cost: float | None = Field(default=None, description="Cost incurred by the client for the action")
    creation_date: dt.datetime | None = Field(default=None, description="Date when the action was created")
    currency: str | None = Field(default=None, description="Currency in which the amounts are denoted")
    customer_area: str | None = Field(default=None, description="Geographical area of the customer")
    customer_city: str | None = Field(default=None, description="City of the customer")
    customer_country: str | None = Field(default=None, description="Country of the customer")
    customer_id: str | None = Field(default=None, description="Unique identifier for the customer")
    customer_post_code: str | None = Field(default=None, description="Postal code of the customer")
    customer_region: str | None = Field(default=None, description="Region of the customer")
    customer_status: str | None = Field(default=None, description="Status of the customer (e.g., new, returning)")
    delta_amount: float | None = Field(default=None, description="Difference in the total amount from the expected value")
    delta_payout: float | None = Field(default=None, description="Difference in the payout amount from the expected value")
    event_code: str | None = Field(default=None, description="Code representing the type of event")
    event_date: dt.datetime | None = Field(default=None, description="Date when the event occurred")
    id: str | None = Field(default=None, description="Unique identifier for the action")
    intended_amount: float | None = Field(default=None, description="Intended total amount involved in the action")
    intended_payout: float | None = Field(default=None, description="Intended payout amount to the media partner")
    ip_address: str | None = Field(default=None, description="IP address associated with the action")
    locking_date: dt.datetime | None = Field(default=None, description="Date when the action was locked")
    media_partner_id: str | None = Field(default=None, description="Unique identifier for the media partner")
    media_partner_name: str | None = Field(default=None, description="Name of the media partner")
    note: str | None = Field(default=None, description="Additional notes or comments regarding the action")
    oid: str | None = Field(default=None, description="Order identifier associated with the action")
    payout: float | None = Field(default=None, description="Payout amount to the media partner")
    promo_code: str | None = Field(default=None, description="Promotional code used in the action")
    referring_date: dt.datetime | None = Field(default=None, description="Date when the action was referred")
    referring_domain: str | None = Field(default=None, description="Domain of the referring source")
    referring_type: str | None = Field(default=None, description="Type of referring source (e.g., website, app)")
    shared_id: str | None = Field(default=None, description="Shared identifier for tracking purposes")
    state: str | None = Field(default=None, description="Current state of the action (e.g., pending, approved)")
    uri: str | None = Field(default=None, description="Uniform Resource Identifier related to the action")
    date: dt.date | None = Field(default=None, description="Partition date")
