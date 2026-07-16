import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Audiences(Schema):
    """First-party and partner audiences of a partner or advertiser."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    name: str | None = Field(default=None, description="The name of the audience")
    first_party_and_partner_audience_id: str | None = Field(
        default=None, description="The unique identifier for the first or partner audience"
    )
    display_name: str | None = Field(default=None, description="The display name of the audience")
    first_party_and_partner_audience_type: str | None = Field(
        default=None, description="The type of audience, indicating whether it is first-party or partner"
    )
    audience_type: str | None = Field(
        default=None, description="The classification of the audience based on its purpose or characteristics"
    )
    audience_source: str | None = Field(default=None, description="The source from which the audience data is derived")
    membership_duration_days: str | None = Field(
        default=None, description="The duration, in days, for which a user remains a member of the audience"
    )
    description: str | None = Field(default=None, description="A detailed description of the audience")

