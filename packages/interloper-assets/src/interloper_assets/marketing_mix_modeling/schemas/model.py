import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class Model(Schema):
    date: dt.date = Field(..., description="The date of the campaign")
