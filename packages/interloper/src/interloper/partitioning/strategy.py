import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionStrategy:
    column: str
    allow_window: bool = False


@dataclass(frozen=True)
class TimePartitionStrategy(PartitionStrategy):
    start_date: dt.date | None = None
