import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PerformanceByDomain(Schema):
    """Performance report summarizing metrics by domain"""

    action_cost: float | None = Field(default=None, description="Cost associated with the actions")
    actions: int | None = Field(default=None, description="Number of actions taken")
    calls: int | None = Field(default=None, description="Number of calls generated")
    clicks: int | None = Field(default=None, description="Number of clicks received")
    cpc: float | None = Field(default=None, description="Cost per click metric")
    cpc_cost: float | None = Field(default=None, description="Cost per click")
    domain: str | None = Field(default=None, description="Domain of the website or publisher")
    impressions: int | None = Field(default=None, description="Number of impressions received")
    network: int | None = Field(default=None, description="Advertising network associated with the domain")
    other_cost: float | None = Field(default=None, description="Other costs incurred")
    revenue: float | None = Field(default=None, description="Revenue generated from the actions")
    total_cost: float | None = Field(default=None, description="Total cost incurred")
    date: dt.date | None = Field(default=None, description="Partition date")
