"""Partitioning: the ordered space an asset is split into, and the window a run covers.

A `PartitionConfig` declares how an asset is partitioned and is the authority on its partition
space: it validates the window a run was given, parses persisted keys, builds a window from two
keys, decides whether another config accepts the same windows, and renders its catalog
definition. A `Partition` is the storage unit and a `PartitionWindow` the run scope: a contiguous,
never empty range of the space, named by its first and last partition. Both render the
`PartitionFilter` a database selects their rows with, so no destination branches on the kind of
partitioning. The time kind lives in `interloper.partitioning.time`, the snapshot kind in
`interloper.partitioning.snapshot`, the list kind in `interloper.partitioning.list`.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any, ClassVar

from interloper.errors import ConfigError, PartitionError


@dataclass(frozen=True)
class PartitionFilter:
    """The rows a partition or a window covers: a column, and either values or bounds.

    Exactly one of ``values`` and ``bounds`` is set. A backend renders it in its dialect:
    ``column IN (...)``, or ``column >= start AND column < end``.

    Attributes:
        column: The partition column.
        values: The values the rows carry, for partitions matched by membership.
        bounds: The ``(start, end)`` of a time range, ``end`` excluded.
    """

    column: str
    values: tuple[Any, ...] | None = None
    bounds: tuple[Any, Any] | None = None

    def __post_init__(self) -> None:
        """Require exactly one of ``values`` and ``bounds``.

        Raises:
            ValueError: If both or neither are set.
        """
        if (self.values is None) == (self.bounds is None):
            raise ValueError("A partition filter carries exactly one of values and bounds.")


class Partition(ABC):
    """The storage unit: one slice of a partitioned asset, named by its key.

    Concrete kinds are frozen dataclasses carrying the partition's ``value`` (a period start for
    a time partition, a declared string for a list one); the base declares the contract and
    types that one attribute every kind carries, so ``context.partition.value`` reads without
    narrowing. ``str(partition)`` is the key, which is what reaches destination paths, run rows
    and filters.
    """

    value: Any

    @property
    @abstractmethod
    def key(self) -> str:
        """The partition's key."""

    @property
    @abstractmethod
    def window(self) -> PartitionWindow:
        """The one-partition window covering only this partition."""

    @abstractmethod
    def filter(self, column: str) -> PartitionFilter:
        """The rows this partition covers.

        Args:
            column: The partition column the rows carry.

        Returns:
            The filter a database selects the partition's rows with.
        """

    @abstractmethod
    def slice(self, data: Any, column: str) -> Any:
        """Return this partition's slice of *data*.

        Args:
            data: The table to slice, in any registered representation's type.
            column: Name of the partition column to select on.

        Returns:
            The partition's slice of the data.
        """

    def __str__(self) -> str:
        """Return the partition's key.

        Returns:
            The key.
        """
        return self.key


@dataclass(frozen=True)
class PartitionWindow(ABC):
    """The run scope: a contiguous, never empty range of a partition space.

    Named by its first and last partition's values; the concrete kind enumerates what lies
    between. ``str(window)`` is the single partition's key for a window of one, and
    ``"first..last"`` otherwise.

    Attributes:
        start: The first partition's value.
        end: The last partition's value.
    """

    start: Any
    end: Any

    @property
    @abstractmethod
    def first(self) -> Partition:
        """The window's first partition."""

    @property
    @abstractmethod
    def last(self) -> Partition:
        """The window's last partition."""

    @property
    @abstractmethod
    def partitions(self) -> list[Partition]:
        """Every partition of the window, first to last."""

    @abstractmethod
    def __len__(self) -> int:
        """Count the window's partitions.

        Returns:
            The number of partitions, at least 1.
        """

    @abstractmethod
    def filter(self, column: str) -> PartitionFilter:
        """The rows the whole window covers.

        Args:
            column: The partition column the rows carry.

        Returns:
            The filter a database selects the window's rows with.
        """

    def __iter__(self) -> Iterator[Partition]:
        """Iterate the partitions, first to last.

        Returns:
            An iterator over `partitions`.
        """
        return iter(self.partitions)

    def __str__(self) -> str:
        """Render the window by its partition keys.

        Returns:
            The single key for a window of one, else ``"first..last"``.
        """
        if len(self) == 1:
            return self.first.key
        return f"{self.first.key}..{self.last.key}"

    @classmethod
    def of(cls, scope: Partition | PartitionWindow | None) -> PartitionWindow | None:
        """The window a run argument denotes.

        The entry points accept a bare partition as sugar for the window of one it names; inside
        the framework there is one scope, the window.

        Args:
            scope: A partition, a window, or ``None`` for an unscoped run.

        Returns:
            The partition's one-partition window, the window as given, or ``None``.
        """
        return scope.window if isinstance(scope, Partition) else scope


@dataclass(frozen=True, kw_only=True)
class PartitionConfig(ABC):
    """How an asset is partitioned: the authority on its partition space.

    Declared on an asset. Every concrete kind names itself through ``kind`` and is registered at
    class creation, so a catalog definition rebuilds the right one through `from_definition`.
    The check messages read as predicates of the asset, so callers prefix them with its name.

    ``windowed`` says whether the partitions a run writes are the ones its window names. They
    are for every kind but the snapshot, which writes the period it observed in whatever the
    window (see `effective_window`), so a scheduler neither sizes a window by it nor runs it
    once per partition of one.

    Attributes:
        column: The column the rows carry the partition value in.
        allow_window: Whether one run may cover several partitions.
    """

    kind: ClassVar[str]
    windowed: ClassVar[bool] = True
    _kinds: ClassVar[dict[str, type[PartitionConfig]]] = {}

    column: str
    allow_window: bool = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Register a concrete kind under its ``kind`` name.

        Args:
            **kwargs: Class-creation keyword arguments, forwarded untouched.
        """
        super().__init_subclass__(**kwargs)
        if "kind" in cls.__dict__:
            PartitionConfig._kinds[cls.kind] = cls

    def __str__(self) -> str:
        """Describe the partitioning, as an error message names it.

        Returns:
            ``partitioned by <kind>``; a kind refines it.
        """
        return f"partitioned by {self.kind}"

    def check(self, window: PartitionWindow | None) -> None:
        """Validate the window a run was given against this partitioning.

        Args:
            window: The run's window, ``None`` when the run was unscoped.

        Raises:
            PartitionError: If no window was given, if several partitions were given and windows
                are not allowed, or if the kind-specific check fails.
        """
        if window is None:
            raise PartitionError("is partitioned, but the run was given no window")
        if len(window) > 1 and not self.allow_window:
            raise PartitionError(
                "does not allow windows (allow_window=False), but the run was given several partitions"
            )
        self._check(window)

    @abstractmethod
    def _check(self, window: PartitionWindow) -> None:
        """Validate a window against the kind.

        Args:
            window: The run's window, never ``None`` and within ``allow_window``.
        """

    def effective_window(self, window: PartitionWindow | None, observed_at: dt.datetime) -> PartitionWindow | None:
        """The window an asset of this partitioning consumes in a run.

        Args:
            window: The run's window, ``None`` when the run was unscoped.
            observed_at: The run's observation instant, aware; a kind that writes the period it
                observed in reads it (see ``windowed``).

        Returns:
            The run's window, unchanged.
        """
        return window

    @abstractmethod
    def partition(self, key: str) -> Partition:
        """Parse a persisted key into the partition it names.

        Args:
            key: The partition key.

        Returns:
            The partition.
        """

    def window(self, start_key: str, end_key: str) -> PartitionWindow:
        """Build the window from its first and last key.

        Args:
            start_key: The first partition's key.
            end_key: The last partition's key.

        Returns:
            The window covering both keys and everything between.
        """
        return self._window(self.partition(start_key), self.partition(end_key))

    @abstractmethod
    def _window(self, first: Partition, last: Partition) -> PartitionWindow:
        """Build the window between two parsed partitions.

        Args:
            first: The first partition.
            last: The last partition.

        Returns:
            The window.
        """

    @abstractmethod
    def compatible(self, other: PartitionConfig) -> bool:
        """Whether *other* accepts exactly the windows this config accepts.

        Args:
            other: The partitioning at the other end of a DAG edge.

        Returns:
            True when a run's window is valid for both.
        """

    def to_definition(self) -> dict[str, Any]:
        """Render the catalog definition of this partitioning.

        Returns:
            The fields, plus ``kind``.
        """
        return {"kind": self.kind, **asdict(self)}

    @classmethod
    def from_definition(cls, definition: Mapping[str, Any]) -> PartitionConfig:
        """Rebuild a partitioning from its catalog definition.

        Args:
            definition: The mapping `to_definition` rendered, possibly after a JSON round trip.

        Returns:
            The partitioning, of the kind the definition names.

        Raises:
            ConfigError: If the definition names no registered kind, or carries
                a field its kind does not declare.
        """
        kind = definition.get("kind")
        config_class = cls._kinds.get(kind) if isinstance(kind, str) else None
        if config_class is None:
            raise ConfigError(f"Partitioning definition names no known kind: {kind!r} (known: {sorted(cls._kinds)})")
        arguments = {name: value for name, value in definition.items() if name != "kind"}
        unknown = sorted(arguments.keys() - {field.name for field in fields(config_class) if field.init})
        if unknown:
            raise ConfigError(f"Partitioning definition of kind {kind!r} has fields it does not declare: {unknown}")
        return config_class(**arguments)
