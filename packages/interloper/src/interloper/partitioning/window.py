import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from interloper.partitioning.partition import Partition, TimePartition
from interloper.utils.dates import date_range


@dataclass(frozen=True)
class PartitionWindow(ABC):
    start: Any
    end: Any

    @abstractmethod
    def __iter__(self) -> Generator[Partition]:
        pass


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    start: dt.date
    end: dt.date

    def __iter__(self) -> Generator[TimePartition]:
        yield from self.iterate()

    def __repr__(self):
        return f"{self.start.isoformat()} to {self.end.isoformat()}"

    def iterate(self) -> Generator[TimePartition]:
        for date in date_range(self.start, self.end, reversed=True):
            yield TimePartition(date)

    def partition_count(self) -> int:
        return (self.end - self.start).days + 1
