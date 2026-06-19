import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DemographicsStatsByCountry(Schema):
    """Instagram follower demographics broken down by country."""

    date: dt.date = Field(description="The date of the record")
    dimension: str = Field(description="The country dimension value")
    name: str = Field(description="The metric name")
    value: int = Field(description="The metric value")


class DemographicsStatsByCity(Schema):
    """Instagram follower demographics broken down by city."""

    date: dt.date = Field(description="The date of the record")
    dimension: str = Field(description="The city dimension value")
    name: str = Field(description="The metric name")
    value: int = Field(description="The metric value")


class DemographicsStatsByAgeGender(Schema):
    """Instagram follower demographics broken down by age and gender."""

    date: dt.date = Field(description="The date of the record")
    dimension: str = Field(description="The age and gender dimension value")
    name: str = Field(description="The metric name")
    value: int = Field(description="The metric value")
