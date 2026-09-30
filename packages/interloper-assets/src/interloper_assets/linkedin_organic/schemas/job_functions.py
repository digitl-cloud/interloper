import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class JobFunctions(Schema):
    """LinkedIn job function taxonomy. One row per job function and day, with its localized name; resolves urn:li:function values."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: int | None = Field(default=None, description="The job function identifier (the id of urn:li:function).")
    name_localized_en_us: str | None = Field(default=None, description="The job function name in the en_US locale.")
    name_preferred_locale_country: str | None = Field(
        default=None, description="The country of the name's preferred locale."
    )
    name_preferred_locale_language: str | None = Field(
        default=None, description="The language of the name's preferred locale."
    )
