import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class ConsentsStats(Schema):
    """The Usercentrics granular report provides daily consent metrics per data processing service (DPS) within the Usercentrics consent management platform, broken down by settings configuration, country, operating system and browser. It includes the number of consents given, the number of consent actions such as accept or reject, and the total number of user interactions."""

    day: dt.date | None = Field(default=None, description="The date for which the data is recorded (YYYY-MM-DD).")
    settings_id: str | None = Field(default=None, description="Unique identifier for the Usercentrics settings configuration.")
    dps_id: str | None = Field(default=None, description="Unique identifier for the data processing service (DPS).")
    consent: int | None = Field(default=None, description="Number of consents given for the specified DPS on the given day.")
    action: int | None = Field(default=None, description="Number of actions (such as accept or reject) performed for the DPS.")
    total: int | None = Field(default=None, description="Total number of user interactions recorded for the DPS.")
    country: str | None = Field(default=None, description="Country code (ISO 3166-1 alpha-2) of the user.")
    os: str | None = Field(default=None, description="Operating system of the user's device.")
    browser: str | None = Field(default=None, description="Web browser used by the user.")
