import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionConfig:
    column: str
    allow_window: bool = False


@dataclass(frozen=True)
class TimePartitionConfig(PartitionConfig):
    start_date: dt.date | None = None
