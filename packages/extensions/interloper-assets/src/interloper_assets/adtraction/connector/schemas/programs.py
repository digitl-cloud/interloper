import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Programs(Schema):
    """The advertiser's partner program on Adtraction."""

    program_id: str | None = Field(default=None, description="Numerical Adtraction ID of the partner program")
    program_name: str | None = Field(default=None, description="Name of the partner program")
    currency: str | None = Field(default=None, description="Currency of the program's amounts, in ISO 4217 format")
    market: str | None = Field(default=None, description="Market of the program, as an ISO 3166-1 alpha-2 country code")
    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
