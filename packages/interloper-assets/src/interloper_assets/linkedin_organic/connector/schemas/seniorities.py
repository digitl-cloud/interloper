import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Seniorities(Schema):
    """LinkedIn seniority level taxonomy. One row per seniority level and day, with its localized name; resolves urn:li:seniority values."""

    date: dt.date | None = Field(
        default=None, description="The day the snapshot was taken (stamped from the partition)."
    )
    id: int | None = Field(default=None, description="The seniority level identifier (the id of urn:li:seniority).")
    name_localized_en_us: str | None = Field(default=None, description="The seniority level name in the en_US locale.")
    name_preferred_locale_country: str | None = Field(
        default=None, description="The country of the name's preferred locale."
    )
    name_preferred_locale_language: str | None = Field(
        default=None, description="The language of the name's preferred locale."
    )
