from interloper.schema import Schema
from pydantic import Field


class Events(Schema):
    """The GA4 BigQuery export, one row per event, sharded daily into events_YYYYMMDD tables."""

    event_date: str | None = Field(default=None, description="The event's day as YYYYMMDD in the property's timezone.")
    event_timestamp: int | None = Field(
        default=None, description="When the event was logged, in microseconds since the epoch."
    )
    event_name: str | None = Field(default=None, description="The event's name (page_view, session_start, ...).")
    user_pseudo_id: str | None = Field(default=None, description="The pseudonymous user id.")
    platform: str | None = Field(default=None, description="The platform the event came from (WEB, ANDROID, IOS).")
    stream_id: str | None = Field(default=None, description="The data stream the event belongs to.")
