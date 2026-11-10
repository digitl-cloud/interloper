"""List partitioning: a finite, ordered set of declared values, such as regions or markets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

from interloper.errors import PartitionError
from interloper.partitioning.base import Partition, PartitionConfig, PartitionFilter, PartitionWindow


@dataclass(frozen=True)
class ListPartition(Partition):
    """One declared value of a list partitioning.

    Attributes:
        value: The declared value, which is also the partition's key.
    """

    value: str

    @property
    def key(self) -> str:
        """The partition's key: its value."""
        return self.value

    @property
    def window(self) -> ListPartitionWindow:
        """The one-partition window covering only this value."""
        return ListPartitionWindow(self.value, self.value, (self.value,))

    def filter(self, column: str) -> PartitionFilter:
        """The rows carrying this value.

        Args:
            column: The partition column the rows carry.

        Returns:
            A membership filter on the one value.
        """
        return PartitionFilter(column, values=(self.value,))

    def slice(self, data: Any, column: str) -> Any:
        """Return the rows whose *column* equals this value.

        Args:
            data: The table to slice, in any registered representation's type.
            column: Name of the partition column to select on.

        Returns:
            The partition's slice of the data.
        """
        from interloper.representation import TableData

        return TableData.from_data(data).filter_eq(column, self.value)


@dataclass(frozen=True)
class ListPartitionWindow(PartitionWindow):
    """A contiguous range of declared values, in their declared order.

    Attributes:
        start: The first value.
        end: The last value.
        values: The declared values the range is taken from, in order.
    """

    start: str
    end: str
    values: tuple[str, ...]

    def __post_init__(self) -> None:
        """Normalise ``values`` to a tuple and check the bounds against it.

        Raises:
            ValueError: If a bound is not a declared value, or the window ends before it starts.
        """
        object.__setattr__(self, "values", tuple(self.values))
        for bound in (self.start, self.end):
            if bound not in self.values:
                raise ValueError(f"List partition window bound {bound!r} is not one of the declared values.")
        if self.values.index(self.start) > self.values.index(self.end):
            raise ValueError(f"List partition window ends before it starts: {self.start!r} to {self.end!r}.")

    @property
    def first(self) -> ListPartition:
        """The window's first partition."""
        return ListPartition(self.start)

    @property
    def last(self) -> ListPartition:
        """The window's last partition."""
        return ListPartition(self.end)

    @property
    def partitions(self) -> list[Partition]:
        """Every value from ``start`` to ``end`` inclusive, as partitions."""
        first, last = self.values.index(self.start), self.values.index(self.end)
        return [ListPartition(value) for value in self.values[first : last + 1]]

    def __len__(self) -> int:
        """Count the window's partitions.

        Returns:
            The number of values from ``start`` to ``end`` inclusive.
        """
        return self.values.index(self.end) - self.values.index(self.start) + 1

    def filter(self, column: str) -> PartitionFilter:
        """The rows carrying any of the window's values.

        Args:
            column: The partition column the rows carry.

        Returns:
            A membership filter on every value of the window.
        """
        return PartitionFilter(column, values=tuple(partition.key for partition in self.partitions))


@dataclass(frozen=True, kw_only=True)
class ListPartitionConfig(PartitionConfig):
    """Partitioning by a declared, ordered list of values.

    Attributes:
        values: The declared values, in order. A window is a contiguous range of them.
    """

    kind: ClassVar[str] = "list"

    values: tuple[str, ...]

    def __post_init__(self) -> None:
        """Normalise ``values`` to a tuple and refuse an unusable list.

        Raises:
            TypeError: If ``values`` is a string rather than a sequence of them.
            ValueError: If the list is empty, holds a duplicate, or holds an
                empty string.
        """
        if isinstance(self.values, str):
            raise TypeError(f"List partitioning values are a sequence of values, not the string {self.values!r}.")
        values = tuple(self.values)
        object.__setattr__(self, "values", values)
        if not values:
            raise ValueError("List partitioning declares at least one value.")
        if len(set(values)) != len(values):
            raise ValueError(f"List partitioning declares a duplicate value: {values!r}.")
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError(f"List partitioning values are non-empty strings: {values!r}.")

    def __str__(self) -> str:
        """Describe the partitioning, as an error message names it.

        Returns:
            ``partitioned by list [...]``, the declared values in order.
        """
        return f"partitioned by list {list(self.values)}"

    @property
    def partitions(self) -> list[Partition]:
        """Every declared value, in order, as partitions."""
        return [ListPartition(value) for value in self.values]

    def _check(self, window: PartitionWindow) -> None:
        """Require a list window whose every value is declared.

        Args:
            window: The run's window.

        Raises:
            PartitionError: If the window is not a list window, or holds an undeclared value.
        """
        if not isinstance(window, ListPartitionWindow):
            raise PartitionError(f"expects a list window, but the run was given a {type(window).__name__}")
        for partition in window:
            if partition.key not in self.values:
                raise PartitionError(f"declares no partition {partition.key!r} (declared: {list(self.values)})")

    def partition(self, key: str) -> ListPartition:
        """Parse a key into the declared partition it names.

        Args:
            key: The partition key.

        Returns:
            The partition.

        Raises:
            PartitionError: If the key is not a declared value.
        """
        if key not in self.values:
            raise PartitionError(f"'{key}' is not one of the declared partitions {list(self.values)}")
        return ListPartition(key)

    def _window(self, first: Partition, last: Partition) -> ListPartitionWindow:
        """Build the window between two declared partitions.

        Args:
            first: The first partition.
            last: The last partition.

        Returns:
            The window.

        Raises:
            PartitionError: If *last* comes before *first* in the declared order.
        """
        if self.values.index(last.key) < self.values.index(first.key):
            raise PartitionError(f"window ends before it starts: {first.key!r} to {last.key!r}")
        return ListPartitionWindow(first.key, last.key, self.values)

    def compatible(self, other: PartitionConfig) -> bool:
        """Whether *other* is a list partitioning over the same values.

        Args:
            other: The partitioning at the other end of a DAG edge.

        Returns:
            True when both declare the same values in the same order.
        """
        return isinstance(other, ListPartitionConfig) and other.values == self.values
