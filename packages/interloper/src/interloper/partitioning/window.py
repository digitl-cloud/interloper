"""This module contains the partition window classes."""
import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from interloper.partitioning.partition import Partition, TimePartition
from interloper.utils.dates import date_range


@dataclass(frozen=True)
class PartitionWindow(ABC):
    """A window of partitions.

    Attributes:
        start: The start of the window.
        end: The end of the window.
    """

    start: Any
    end: Any

    @abstractmethod
    def __iter__(self) -> Generator[Partition]:
        """Iterate over the partitions in the window."""
        pass


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    """A window of time-based partitions.

    Attributes:
        start: The start date of the window.
        end: The end date of the window.
    """

    start: dt.date
    end: dt.date

    def __iter__(self) -> Generator[TimePartition]:
        """Iterate over the partitions in the window.

        Yields:
            The partitions in the window.
        """
        yield from self.iter_partitions()

    def __repr__(self):
        """Return a string representation of the partition window."""
        return f"{self.start.isoformat()} to {self.end.isoformat()}"

    def iter_partitions(self) -> Generator[TimePartition]:
        """Iterate over the partitions in the window.

        Yields:
            The partitions in the window.
        """
        for date in date_range(self.start, self.end, reversed=True):
            yield TimePartition(date)

    def partition_count(self) -> int:
        """The number of partitions in the window.

        Returns:
            The number of partitions in the window.
        """
        return (self.end - self.start).days + 1
