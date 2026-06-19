import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PerformanceStatsByIo(Schema):
    """Performance report detailing metrics by IO"""

    action_cost: float | None = Field(default=None, description="Cost associated with the actions")
    actions: int | None = Field(default=None, description="Number of actions taken")
    calls: int | None = Field(default=None, description="Number of calls generated")
    clicks: int | None = Field(default=None, description="Number of clicks received")
    client_cost: float | None = Field(default=None, description="Cost incurred by the client")
    cpc: float | None = Field(default=None, description="Cost per click metric")
    cpc_cost: float | None = Field(default=None, description="Cost per click")
    impressions: int | None = Field(default=None, description="Number of impressions received")
    io: str | None = Field(default=None, description="Insertion Order identifier")
    mp_count: int | None = Field(default=None, description="Count of media partners involved")
    network: str | None = Field(default=None, description="Advertising network associated with the insertion order")
    other_cost: float | None = Field(default=None, description="Other costs incurred")
    revenue: float | None = Field(default=None, description="Revenue generated from the actions")
    total_cost: float | None = Field(default=None, description="Total cost incurred")
    date: dt.date | None = Field(default=None, description="Partition date")
