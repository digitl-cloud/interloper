"""This module contains the partition classes."""
import datetime as dt
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Partition:
    """A partition of an asset.

    Attributes:
        value: The value of the partition.
    """

    value: Any

    @property
    def id(self) -> str:
        """The unique identifier of the partition."""
        return str(self.value)


@dataclass(frozen=True)
class TimePartition(Partition):
    """A time-based partition of an asset.

    Attributes:
        value: The date of the partition.
    """

    value: dt.date

    def __repr__(self):
        """Return a string representation of the partition."""
        return self.value.isoformat()

    @property
    def id(self) -> str:
        """The unique identifier of the partition."""
        return self.value.isoformat()
