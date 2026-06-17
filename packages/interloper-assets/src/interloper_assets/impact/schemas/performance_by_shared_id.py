import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PerformanceBySharedId(Schema):
    """Performance report summarizing metrics by partner shared ID"""

    media: str | None = Field(default=None, description="Media (partner) name")
    shared_id: str | None = Field(default=None, description="Partner shared ID")
    impressions: int | None = Field(default=None, description="Number of impressions received")
    clicks: int | None = Field(default=None, description="Number of clicks received")
    actions: int | None = Field(default=None, description="Number of actions taken")
    revenue: float | None = Field(default=None, description="Revenue generated from the actions")
    action_cost: float | None = Field(default=None, description="Cost associated with the actions")
    calls: int | None = Field(default=None, description="Number of calls generated")
    cpc_cost: float | None = Field(default=None, description="Cost per click")
    click_cost: float | None = Field(default=None, description="Cost associated with clicks")
    other_cost: float | None = Field(default=None, description="Other costs incurred")
    total_cost: float | None = Field(default=None, description="Total cost incurred")
    cpc: float | None = Field(default=None, description="Cost per click metric")
    date: dt.date | None = Field(default=None, description="Partition date")
