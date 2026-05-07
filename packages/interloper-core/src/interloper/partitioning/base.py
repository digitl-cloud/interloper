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
