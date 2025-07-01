"""This module contains the partition configuration classes."""
import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class PartitionConfig:
    """The configuration for a partition.

    Attributes:
        column: The name of the partitioning column.
        allow_window: Whether to allow windowed partitions.
    """

    column: str
    allow_window: bool = False


@dataclass(frozen=True)
class TimePartitionConfig(PartitionConfig):
    """The configuration for a time partition.

    Attributes:
        start_date: The start date of the partition.
    """

    start_date: dt.date | None = None
