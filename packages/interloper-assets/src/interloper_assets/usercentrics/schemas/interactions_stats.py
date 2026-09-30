import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class InteractionsStats(Schema):
    """The Usercentrics interaction report provides daily counts of user interactions with the Usercentrics consent management platform, broken down by event type, settings configuration, host, country, browser, device type and operating system."""

    day: dt.date | None = Field(default=None, description="The date of the interaction (YYYY-MM-DD).")
    event_type: int | None = Field(default=None, description="The type of event (e.g., page view, click).")
    settings_id: str | None = Field(default=None, description="Unique identifier for the Usercentrics settings configuration.")
    host: str | None = Field(default=None, description="The host of the interaction (e.g., www.example.com).")
    country: str | None = Field(default=None, description="Country code (ISO 3166-1 alpha-2) of the user.")
    browser: str | None = Field(default=None, description="Web browser used by the user.")
    device_type: str | None = Field(default=None, description="Type of device used (e.g., desktop, mobile).")
    os: str | None = Field(default=None, description="Operating system of the user's device.")
    number: int | None = Field(default=None, description="The number of interactions recorded for this combination of day and dimensions.")
