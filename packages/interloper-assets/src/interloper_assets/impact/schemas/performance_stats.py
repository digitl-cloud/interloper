import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class PerformanceStats(Schema):
    """Performance report detailing various metrics for media performance"""

    action_cost: float | None = Field(default=None, description="Cost associated with actions")
    actions: int | None = Field(default=None, description="Number of actions taken")
    calls: int | None = Field(default=None, description="Number of calls generated")
    clickc_cost: float | None = Field(default=None, description="Cost associated with clicks")
    clicks: int | None = Field(default=None, description="Number of clicks received")
    cpc: float | None = Field(default=None, description="Cost per click metric")
    cpc_cost: float | None = Field(default=None, description="Cost per click")
    date_display: str | None = Field(default=None, description="Date displayed in the report")
    date_sort: str | None = Field(default=None, description="Date used for sorting entries")
    impressions: int | None = Field(default=None, description="Number of impressions received")
    media_count: int | None = Field(default=None, description="Count of media items for the given date")
    other_cost: float | None = Field(default=None, description="Other costs incurred")
    revenue: float | None = Field(default=None, description="Revenue generated")
    total_cost: float | None = Field(default=None, description="Total cost incurred")
    date: dt.date | None = Field(default=None, description="Partition date")
