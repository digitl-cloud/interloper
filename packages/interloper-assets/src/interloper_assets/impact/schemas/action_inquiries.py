import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ActionInquiries(Schema):
    """Impact.com inquiries report detailing inquiries and their associated attributes"""

    action_id: str | None = Field(default=None, description="Unique identifier for the action associated with the inquiry")
    action_uri: str | None = Field(default=None, description="URI for the action associated with the inquiry")
    auto_approval_date: dt.datetime | None = Field(default=None, description="Date when the inquiry was automatically approved")
    campaign_id: str | None = Field(default=None, description="Unique identifier for the campaign associated with the inquiry")
    campaign_name: str | None = Field(default=None, description="Name of the campaign associated with the inquiry")
    comments: str | None = Field(default=None, description="Comments related to the inquiry")
    creation_date: dt.datetime | None = Field(default=None, description="Date when the inquiry was created")
    expected_payout: float | None = Field(default=None, description="Expected payout amount for the inquiry")
    final_payout: str | None = Field(default=None, description="Final payout amount for the inquiry")
    id: str | None = Field(default=None, description="Unique identifier for the inquiry record")
    inquiry_type: str | None = Field(default=None, description="Type of inquiry (e.g., dispute, question)")
    media_partner_id: str | None = Field(default=None, description="Unique identifier for the media partner associated with the inquiry")
    media_partner_name: str | None = Field(default=None, description="Name of the media partner associated with the inquiry")
    order_id: str | None = Field(default=None, description="Unique identifier for the order associated with the inquiry")
    reject_reason: str | None = Field(default=None, description="Reason for rejecting the inquiry, if applicable")
    resolution_date: dt.datetime | None = Field(default=None, description="Date when the inquiry was resolved")
    resolution_deadline_date: dt.datetime | None = Field(default=None, description="Deadline date for resolving the inquiry")
    resolution_status: str | None = Field(default=None, description="Current resolution status of the inquiry")
    tracking_link: str | None = Field(default=None, description="Tracking link associated with the inquiry")
    transaction_amount: float | None = Field(default=None, description="Amount of the transaction associated with the inquiry")
    transaction_date: dt.datetime | None = Field(default=None, description="Date when the transaction occurred")
    uri: str | None = Field(default=None, description="General URI related to the inquiry")
    date: dt.date | None = Field(default=None, description="Partition date")
