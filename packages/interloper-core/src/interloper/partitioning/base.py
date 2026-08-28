"""Abstract base classes for partitioning."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PartitionConfig:
    """Configuration for how an asset is partitioned.

    Attributes:
        column: Column used for partitioning.
        allow_window: Whether windowed partitions are allowed.
    """

    column: str
    allow_window: bool = False


@dataclass(frozen=True)
class Partition(ABC):
    """A single partition of an asset."""

    value: Any

    @property
    def id(self) -> str:
        """Unique identifier derived from the partition value."""
        return str(self.value)

    def slice(self, data: Any, column: str) -> Any:
        """Return this partition's slice of *data*, selecting by id equality on *column*.

        Data no registered representation recognizes passes through unchanged
        since it cannot be split.

        Returns:
            The partition's slice of the data.
        """
        from interloper.representation import Representation

        representation = Representation.of(data)
        if not representation.matches(data):
            return data
        return representation.filter_eq(data, column, self.id)


@dataclass(frozen=True)
class PartitionWindow(ABC):
    """A contiguous range of partitions defined by start and end bounds."""

    start: Any
    end: Any

    @property
    def id(self) -> str:
        """Unique identifier derived from start and end bounds."""
        return f"{self.start}-{self.end}"

    @abstractmethod
    def __iter__(self) -> Iterator[Partition]:
        """Iterate over the partitions in the window."""
