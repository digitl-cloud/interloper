import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FactDailyEvents(Schema):
    """Events and users per day and event name, from the GA4 export."""

    date: dt.date | None = Field(default=None, description="The event day.")
    event_name: str | None = Field(default=None, description="The event's name.")
    events: int | None = Field(default=None, description="Events logged that day.")
    users: int | None = Field(default=None, description="Distinct pseudonymous users that day.")
