import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class CustomAudiences(Schema):
    """Detail of a single first-party/partner audience, including per-surface audience sizes."""

    name: str | None = Field(default=None, description="The name of the custom audience.")
    first_party_and_partner_audience_id: str | None = Field(
        default=None, description="The unique identifier for the first and partner audience."
    )
    display_name: str | None = Field(default=None, description="The display name of the custom audience.")
    first_party_and_partner_audience_type: str | None = Field(
        default=None, description="The type of the first and partner audience."
    )
    audience_type: str | None = Field(default=None, description="The type of audience, such as custom or predefined.")
    audience_source: str | None = Field(
        default=None, description="The source of the audience data, such as first-party or third-party."
    )
    membership_duration_days: str | None = Field(
        default=None, description="The duration in days for which a user remains a member of the audience."
    )
    display_audience_size: str | None = Field(
        default=None, description="The total size of the audience for display campaigns."
    )
    active_display_audience_size: str | None = Field(
        default=None, description="The size of the active audience for display campaigns."
    )
    youtube_audience_size: str | None = Field(
        default=None, description="The size of the audience for YouTube campaigns."
    )
    gmail_audience_size: str | None = Field(default=None, description="The size of the audience for Gmail campaigns.")
    display_mobile_web_audience_size: str | None = Field(
        default=None, description="The size of the audience for display campaigns on mobile web."
    )
    display_desktop_audience_size: str | None = Field(
        default=None, description="The size of the audience for display campaigns on desktop."
    )
    description: str | None = Field(default=None, description="A description of the custom audience.")
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
