import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from interloper.core.utils import date_range


#########################
# PARTITIONS
#########################
@dataclass(frozen=True)
class Partition:
    value: Any


@dataclass(frozen=True)
class TimePartition(Partition):
    value: dt.date


#########################
# PARTITION RANGES
#########################
@dataclass(frozen=True)
class PartitionRange(ABC):
    start: Any
    end: Any

    @abstractmethod
    def __iter__(self) -> Generator[Partition]:
        pass


@dataclass(frozen=True)
class TimePartitionRange(PartitionRange):
    start: dt.date
    end: dt.date

    def __iter__(self) -> Generator[TimePartition]:
        for date in date_range(self.start, self.end, reversed=True):
            yield TimePartition(date)


#########################
# PARTITION STRATEGIES
#########################
@dataclass(frozen=True)
class PartitionStrategy:
    column: str
    allow_range: bool = False


@dataclass(frozen=True)
class TimePartitionStrategy(PartitionStrategy):
    start_date: dt.date | None = None
