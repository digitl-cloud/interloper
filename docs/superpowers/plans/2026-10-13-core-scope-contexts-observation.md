# Core phase 1: scope, contexts and observation. Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give interloper-core one run scope (a partition window or nothing), partition kinds that own their space (time and list), three contexts of one shape, and one observed-step primitive, deleting the hand-rolled validators, metadata builders and event triplets they replace.

**Architecture:** `interloper.partitioning` becomes the authority on partition spaces: `PartitionConfig.check/partition/window/compatible/from_definition`, `Partition.filter/slice/window`, `PartitionWindow.partitions/filter`, with `TimePartition*` and the new `ListPartition*` as the two kinds. Every signature along the execution path takes `window: PartitionWindow | None`; `OperationContext`, `ExecutionContext` and `IOContext` all start with `asset, window, metadata`; `Destination.read` returns one table. `Component.event_metadata` builds every event's metadata, `interloper.events.step.Step` wraps a lifecycle step in its started/completed/failed events and span, and `EventLogger` takes the component. The runner's preflight and the asset's own check both call `PartitionConfig.check`.

**Tech Stack:** Python >= 3.10, pydantic 2, dataclasses (`kw_only=True` on the configs), opentelemetry-api, pytest (`uv run pytest`), ruff (`uv run ruff check`), ty (`uv run ty check`).

**Spec:** `docs/superpowers/specs/2026-10-13-core-scope-contexts-observation-design.md`

## Global Constraints

- Never commit, amend or rebase: finished work rests uncommitted in the tree, and Guillaume commits on explicit ask. Every task ends with the tree green for its scope, nothing more.
- The interloper-core suite (`uv run pytest packages/interloper-core`) is green at the end of every task from Task 1 on. The workspace suite (`uv run pytest`) is green at the end of Task 7. `uv run ruff check` and `uv run ty check` are green at the end of every task for the files it touched.
- Persisted contracts do not change: `EventType` string values, the spec YAML shape, partition id formats, the operation event id namespace in `RunState.operation_event_id`, the `x-*` schema extras, the CLI contract `interloper run --format inline --run-id ... [--date | --start-date --end-date]`.
- Every module, class, function and method gets a complete Google-style docstring (`Args:`, `Returns:`, `Raises:`, `Yields:` whenever they apply, private helpers and dunders included), as `.claude/rules/python-style.md` requires. Comments only for a non-obvious why. No em dash character anywhere.
- Names are written out in full (`context`, `window`, `partition`, `destination`), never abbreviated; ruff line length 120.
- Test files mirror the source layout one to one (`src/interloper/<pkg>/<module>.py` to `tests/<pkg>/test_<module>.py`); fold tests into the module's file, never a standalone `test_<feature>.py`.
- The authoring surface keeps `il.TimePartition`, `il.TimePartitionWindow`, `il.TimePartitionConfig`, `il.TimeGranularity`, `context.partition_date`, `context.logger`, `context.partition`, `context.window` and the decorator kwargs; only the renames the spec lists move.
- `MultiProcessRunner`, the pickle `FileDestination`, `CSVDestination` and `MemoryDestination` stay.
- Rename the worktree's branch at the start, never move the worktree directory: `git branch -m claude/interloper-core-refactor-5c8540 refactor/core-scope-contexts`.

## Review Focus

Inputs the spec implies but no task's tests would exercise without the lines added here; each has its test pinned to the owning task:

1. A `ListPartitionWindow` built from a bare `ListPartition` (`values=("eu",)`) must pass `ListPartitionConfig(values=("eu","us")).check`: membership, not identity. Pinned in Task 1 (`test_list.py::TestListPartitionConfig::test_a_window_of_one_built_from_a_partition_passes`).
2. A window handed to an unpartitioned asset must be ignored without a warning, and an unpartitioned asset in a windowed DAG must run. Pinned in Task 2 (`tests/asset/test_base.py::TestWindowValidation::test_an_unpartitioned_asset_ignores_the_window`).
3. `Step.__exit__` must emit the failed event and re-raise when the body raises, and must not emit a completed event. Pinned in Task 5 (`tests/events/test_step.py::TestStep::test_a_raising_body_emits_failed_and_re_raises`).
4. A windowed read whose parts are DataFrames must come back as one frame with a fresh index, and a windowed read of non-tabular data must raise `RepresentationError`, never return a list. Pinned in Task 4 (`tests/destination/test_base.py::TestReadDispatch::test_a_window_of_non_tabular_data_is_an_error`) and Task 7 (`packages/interloper-pandas/tests/test_representation.py::TestDataFrameRepresentation::test_concat_joins_frames_with_a_fresh_index`).
5. The container runners must pass `--date` for a one-partition window and nothing for an unpartitioned operation of a windowed run. Pinned in Task 6 (`packages/interloper-docker/tests/test_runner.py::TestBuildCommand`).

---

### Task 0: Branch and baseline

**Files:** none modified.

- [ ] **Step 1: Rename the branch**

```bash
git branch -m claude/interloper-core-refactor-5c8540 refactor/core-scope-contexts
git status --short
```

Expected: the only untracked files are the spec and this plan.

- [ ] **Step 2: Baseline the core suite**

Run: `uv run pytest packages/interloper-core -q`
Expected: all green. If not, stop and report: the plan assumes a green baseline.

---

### Task 1: Partitioning kinds

**Files:**
- Modify: `packages/interloper-core/src/interloper/errors.py` (the `PartitionError` class)
- Rewrite: `packages/interloper-core/src/interloper/partitioning/base.py`
- Rewrite: `packages/interloper-core/src/interloper/partitioning/time.py`
- Create: `packages/interloper-core/src/interloper/partitioning/list.py`
- Modify: `packages/interloper-core/src/interloper/partitioning/__init__.py`
- Modify: `packages/interloper-core/src/interloper/__init__.py` (exports)
- Modify: `packages/interloper-core/src/interloper/destination/database.py` (import `PartitionFilter`, `_filter`)
- Modify: `packages/interloper-core/src/interloper/destination/__init__.py` (re-export)
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (`definition()` uses `to_definition`)
- Rewrite: `packages/interloper-core/tests/partitioning/test_base.py`
- Modify: `packages/interloper-core/tests/partitioning/test_time.py`
- Create: `packages/interloper-core/tests/partitioning/test_list.py`
- Modify: `packages/interloper-core/tests/destination/test_database.py`, `tests/asset/test_base.py`, `tests/asset/test_context.py`, `tests/runner/test_results.py` (replace the generic partition fakes)

**Interfaces:**
- Produces: `PartitionFilter(column, values=None, bounds=None)`; `Partition.id/window/filter(column)/slice(data, column)`; `PartitionWindow.start/end/first/last/partitions/__len__/__iter__/__str__/filter(column)` and `PartitionWindow.of(scope)`; `PartitionConfig.column/allow_window/kind/check(window)/partition(key)/window(start_key, end_key)/compatible(other)/to_definition()/from_definition(definition)`; `TimePartition.bounds/from_key`; `TimePartitionWindow.granularity/bounds/lookback`; `ListPartition(value)`, `ListPartitionWindow(start, end, values)`, `ListPartitionConfig(column=, values=, allow_window=)` with `.partitions`.
- `PartitionError` is a `ConfigError`.

- [ ] **Step 1: Make `PartitionError` a `ConfigError`**

In `errors.py`, replace the class under `# -- Partitioning ---`:

```python
class PartitionError(ConfigError):
    """A partitioning declaration, a partition key or a run's window is invalid."""
```

- [ ] **Step 2: Write the failing tests for the base contracts and the list kind**

Replace `packages/interloper-core/tests/partitioning/test_base.py` with:

```python
"""Tests for ``interloper.partitioning.base``: the contracts every kind implements, through the list kind."""

from __future__ import annotations

import datetime as dt

import pytest

from interloper.errors import ConfigError
from interloper.partitioning.base import Partition, PartitionConfig, PartitionFilter, PartitionWindow
from interloper.partitioning.list import ListPartition, ListPartitionConfig, ListPartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionConfig, TimePartitionWindow


class TestPartitionFilter:
    def test_carries_values_or_bounds(self):
        assert PartitionFilter("region", values=("eu",)).bounds is None
        assert PartitionFilter("date", bounds=(dt.date(2026, 1, 1), dt.date(2026, 1, 2))).values is None

    def test_rejects_both_and_neither(self):
        with pytest.raises(ValueError, match="exactly one"):
            PartitionFilter("region")
        with pytest.raises(ValueError, match="exactly one"):
            PartitionFilter("region", values=("eu",), bounds=("a", "b"))


class TestPartition:
    def test_str_is_the_id(self):
        assert str(ListPartition("eu")) == "eu"
        assert str(TimePartition(dt.date(2026, 1, 15))) == "2026-01-15"

    def test_window_is_the_window_of_one(self):
        window = ListPartition("eu").window
        assert isinstance(window, PartitionWindow)
        assert list(window) == [ListPartition("eu")]

    def test_the_bases_are_abstract(self):
        with pytest.raises(TypeError):
            Partition()  # ty: ignore[missing-argument]
        with pytest.raises(TypeError):
            PartitionWindow("a", "b")  # ty: ignore[missing-argument]
        with pytest.raises(TypeError):
            PartitionConfig(column="x")  # ty: ignore[missing-argument]


class TestPartitionWindow:
    def test_of_a_partition_is_its_window(self):
        partition = TimePartition(dt.date(2026, 1, 15))
        assert PartitionWindow.of(partition) == partition.window

    def test_of_a_window_is_the_window(self):
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        assert PartitionWindow.of(window) is window

    def test_of_none_is_none(self):
        assert PartitionWindow.of(None) is None

    def test_str_names_one_partition_by_its_id(self):
        assert str(ListPartitionWindow("eu", "eu", ("eu", "us"))) == "eu"
        assert str(TimePartition(dt.date(2026, 1, 15)).window) == "2026-01-15"

    def test_str_names_a_range_by_its_bounds(self):
        assert str(ListPartitionWindow("eu", "apac", ("eu", "us", "apac"))) == "eu..apac"
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 15))
        assert str(window) == "2026-01-01..2026-01-15"

    def test_iteration_and_length_agree(self):
        window = ListPartitionWindow("eu", "apac", ("eu", "us", "apac"))
        assert len(window) == 3
        assert [partition.id for partition in window] == ["eu", "us", "apac"]


class TestPartitionConfig:
    def test_a_window_is_required(self):
        with pytest.raises(ConfigError, match="given no window"):
            ListPartitionConfig(column="region", values=("eu",)).check(None)

    def test_several_partitions_need_allow_window(self):
        config = ListPartitionConfig(column="region", values=("eu", "us"))
        with pytest.raises(ConfigError, match="allow_window=False"):
            config.check(ListPartitionWindow("eu", "us", ("eu", "us")))
        ListPartitionConfig(column="region", values=("eu", "us"), allow_window=True).check(
            ListPartitionWindow("eu", "us", ("eu", "us"))
        )

    def test_window_parses_both_keys(self):
        config = ListPartitionConfig(column="region", values=("eu", "us", "apac"))
        window = config.window("us", "apac")
        assert [partition.id for partition in window] == ["us", "apac"]

    def test_definition_round_trips_through_json_like_values(self):
        config = TimePartitionConfig(column="date", allow_window=True, start=dt.date(2026, 1, 1))
        definition = config.to_definition()
        assert definition["kind"] == "time"
        loosened = {**definition, "granularity": "day", "start": "2026-01-01"}
        assert PartitionConfig.from_definition(loosened) == config

    def test_definition_of_the_list_kind_round_trips(self):
        config = ListPartitionConfig(column="region", values=("eu", "us"))
        assert config.to_definition() == {"kind": "list", "column": "region", "allow_window": False, "values": ("eu", "us")}
        assert PartitionConfig.from_definition({**config.to_definition(), "values": ["eu", "us"]}) == config

    def test_an_unknown_kind_is_a_config_error(self):
        with pytest.raises(ConfigError, match="no known kind"):
            PartitionConfig.from_definition({"column": "x"})
        with pytest.raises(ConfigError, match="no known kind"):
            PartitionConfig.from_definition({"kind": "hash", "column": "x"})

    def test_kinds_are_never_compatible_across(self):
        time = TimePartitionConfig(column="date")
        regions = ListPartitionConfig(column="region", values=("eu",))
        assert time.compatible(regions) is False
        assert regions.compatible(time) is False
```

Create `packages/interloper-core/tests/partitioning/test_list.py`:

```python
"""Tests for ``interloper.partitioning.list``."""

from __future__ import annotations

import pytest

from interloper.errors import PartitionError
from interloper.partitioning.base import PartitionFilter
from interloper.partitioning.list import ListPartition, ListPartitionConfig, ListPartitionWindow

REGIONS = ("eu", "us", "apac")


class TestListPartition:
    def test_id_is_the_value(self):
        assert ListPartition("eu").id == "eu"

    def test_filter_is_membership_of_its_value(self):
        assert ListPartition("eu").filter("region") == PartitionFilter("region", values=("eu",))

    def test_slice_selects_rows_by_equality(self):
        rows = [{"region": "eu", "v": 1}, {"region": "us", "v": 2}, {"region": "eu", "v": 3}]
        assert ListPartition("eu").slice(rows, "region") == [rows[0], rows[2]]

    def test_window_covers_only_itself(self):
        window = ListPartition("eu").window
        assert window == ListPartitionWindow("eu", "eu", ("eu",))


class TestListPartitionWindow:
    def test_partitions_are_the_declared_slice_in_order(self):
        window = ListPartitionWindow("us", "apac", REGIONS)
        assert window.partitions == [ListPartition("us"), ListPartition("apac")]
        assert window.first == ListPartition("us")
        assert window.last == ListPartition("apac")
        assert len(window) == 2

    def test_filter_lists_every_id(self):
        assert ListPartitionWindow("eu", "us", REGIONS).filter("region") == PartitionFilter("region", values=("eu", "us"))

    def test_bounds_must_be_declared_values(self):
        with pytest.raises(ValueError, match="not one of the declared values"):
            ListPartitionWindow("eu", "mars", REGIONS)

    def test_bounds_must_be_in_declared_order(self):
        with pytest.raises(ValueError, match="before it starts"):
            ListPartitionWindow("apac", "eu", REGIONS)

    def test_values_are_normalised_to_a_tuple(self):
        assert ListPartitionWindow("eu", "us", list(REGIONS)).values == REGIONS


class TestListPartitionConfig:
    def test_values_are_normalised_and_validated(self):
        assert ListPartitionConfig(column="region", values=["eu", "us"]).values == ("eu", "us")
        with pytest.raises(ValueError, match="at least one"):
            ListPartitionConfig(column="region", values=())
        with pytest.raises(ValueError, match="duplicate"):
            ListPartitionConfig(column="region", values=("eu", "eu"))
        with pytest.raises(ValueError, match="empty"):
            ListPartitionConfig(column="region", values=("eu", ""))

    def test_partitions_are_every_value_in_order(self):
        config = ListPartitionConfig(column="region", values=REGIONS)
        assert [partition.id for partition in config.partitions] == list(REGIONS)

    def test_partition_parses_a_declared_key(self):
        assert ListPartitionConfig(column="region", values=REGIONS).partition("us") == ListPartition("us")

    def test_partition_rejects_an_undeclared_key(self):
        with pytest.raises(PartitionError, match="'mars' is not one of the declared partitions"):
            ListPartitionConfig(column="region", values=REGIONS).partition("mars")

    def test_window_rejects_an_inverted_range(self):
        with pytest.raises(PartitionError, match="before it starts"):
            ListPartitionConfig(column="region", values=REGIONS).window("apac", "eu")

    def test_check_accepts_a_window_of_declared_values(self):
        config = ListPartitionConfig(column="region", values=REGIONS, allow_window=True)
        config.check(ListPartitionWindow("eu", "us", REGIONS))

    def test_a_window_of_one_built_from_a_partition_passes(self):
        # A bare partition's window carries only its own value; membership is what is checked.
        ListPartitionConfig(column="region", values=REGIONS).check(ListPartition("us").window)

    def test_check_rejects_an_undeclared_partition(self):
        config = ListPartitionConfig(column="region", values=("eu", "us"))
        with pytest.raises(PartitionError, match="'apac' is not one of the declared partitions"):
            config.check(ListPartitionWindow("apac", "apac", ("apac",)))

    def test_check_rejects_a_window_of_another_kind(self):
        import datetime as dt

        from interloper.partitioning.time import TimePartition

        with pytest.raises(PartitionError, match="expects a list window"):
            ListPartitionConfig(column="region", values=REGIONS).check(TimePartition(dt.date(2026, 1, 1)).window)

    def test_compatible_means_the_same_values(self):
        config = ListPartitionConfig(column="region", values=REGIONS)
        assert config.compatible(ListPartitionConfig(column="market", values=REGIONS)) is True
        assert config.compatible(ListPartitionConfig(column="region", values=("eu",))) is False
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/partitioning/test_base.py packages/interloper-core/tests/partitioning/test_list.py -q`
Expected: FAIL with `ModuleNotFoundError: interloper.partitioning.list` and `ImportError: PartitionFilter`.

- [ ] **Step 4: Write `partitioning/base.py`**

Replace the file with:

```python
"""Partitioning: the ordered space an asset is split into, and the window a run covers.

A `PartitionConfig` declares how an asset is partitioned and is the authority on its partition
space: it validates the window a run was given, parses persisted keys, builds a window from two
keys, decides whether another config accepts the same windows, and renders its catalog
definition. A `Partition` is the storage unit and a `PartitionWindow` the run scope: a contiguous,
never empty range of the space, named by its first and last partition. Both render the
`PartitionFilter` a database selects their rows with, so no destination branches on the kind of
partitioning. The time kind lives in `interloper.partitioning.time`, the list kind in
`interloper.partitioning.list`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
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
    """The storage unit: one slice of a partitioned asset, named by its id.

    Concrete kinds are frozen dataclasses carrying the partition's value; the base declares the
    contract only. ``str(partition)`` is the id, which is what reaches destination paths, run
    rows and filters.
    """

    @property
    @abstractmethod
    def id(self) -> str:
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
        """Return the partition's id.

        Returns:
            The id.
        """
        return self.id


@dataclass(frozen=True)
class PartitionWindow(ABC):
    """The run scope: a contiguous, never empty range of a partition space.

    Named by its first and last partition's values; the concrete kind enumerates what lies
    between. ``str(window)`` is the single partition's id for a window of one, and
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
        """Render the window by its partition ids.

        Returns:
            The single id for a window of one, else ``"first..last"``.
        """
        if len(self) == 1:
            return self.first.id
        return f"{self.first.id}..{self.last.id}"

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

    Attributes:
        column: The column the rows carry the partition value in.
        allow_window: Whether one run may cover several partitions.
    """

    kind: ClassVar[str]
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
            ConfigError: If the definition names no registered kind.
        """
        kind = definition.get("kind")
        config_cls = cls._kinds.get(kind) if isinstance(kind, str) else None
        if config_cls is None:
            raise ConfigError(f"Partitioning definition names no known kind: {kind!r} (known: {sorted(cls._kinds)})")
        return config_cls(**{name: value for name, value in definition.items() if name != "kind"})
```

- [ ] **Step 5: Write `partitioning/list.py`**

```python
"""List partitioning: a finite, ordered set of declared values, such as regions or markets."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

from interloper.errors import PartitionError
from interloper.partitioning.base import Partition, PartitionConfig, PartitionFilter, PartitionWindow


@dataclass(frozen=True)
class ListPartition(Partition):
    """One declared value of a list partitioning.

    Attributes:
        value: The declared value, which is also the partition's id.
    """

    value: str

    @property
    def id(self) -> str:
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
        from interloper.representation import Representation

        return Representation.of(data).filter_eq(column, self.value)


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
        return PartitionFilter(column, values=tuple(partition.id for partition in self.partitions))


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
            ValueError: If the list is empty, holds a duplicate, or holds an empty string.
        """
        values = tuple(self.values)
        object.__setattr__(self, "values", values)
        if not values:
            raise ValueError("List partitioning declares at least one value.")
        if len(set(values)) != len(values):
            raise ValueError(f"List partitioning declares a duplicate value: {values!r}.")
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError(f"List partitioning values are non-empty strings: {values!r}.")

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
            if partition.id not in self.values:
                raise PartitionError(f"'{partition.id}' is not one of the declared partitions {list(self.values)}")

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
        if self.values.index(last.id) < self.values.index(first.id):
            raise PartitionError(f"window ends before it starts: {first.id!r} to {last.id!r}")
        return ListPartitionWindow(first.id, last.id, self.values)

    def compatible(self, other: PartitionConfig) -> bool:
        """Whether *other* is a list partitioning over the same values.

        Args:
            other: The partitioning at the other end of a DAG edge.

        Returns:
            True when both declare the same values in the same order.
        """
        return isinstance(other, ListPartitionConfig) and other.values == self.values

```

- [ ] **Step 6: Write `partitioning/time.py`**

Replace the file with:

```python
"""Time partitioning: periods identified by their start, at a declared granularity.

A time partition is a **period identified by its start**: the value is the period's first
instant, and the `TimeGranularity` says how long the period lasts. The granularities an asset may
declare are the ones BigQuery time partitioning offers: hourly, daily, monthly, yearly. Every
piece of time arithmetic goes through the granularity, so nothing outside this module hardcodes a
period length.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from interloper.errors import PartitionError
from interloper.partitioning.base import Partition, PartitionConfig, PartitionFilter, PartitionWindow
from interloper.utils import add_months, coerce_to_date, coerce_to_datetime

# -- Granularity ---------------------------------------------------------------


class TimeGranularity(str, Enum):
    """The length of the period one time partition covers.

    Partition ids are ISO-8601 prefixes (``2026``, ``2026-08``, ``2026-08-21``, ``2026-08-21T13``):
    they sort chronologically as strings, embed in paths unchanged, and each shape names its
    granularity, which is what `TimePartition.from_key` relies on.
    """

    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"

    @property
    def key_format(self) -> str:
        """The ``strptime`` shape of this granularity's partition id."""
        return {
            TimeGranularity.HOUR: "%Y-%m-%dT%H",
            TimeGranularity.DAY: "%Y-%m-%d",
            TimeGranularity.MONTH: "%Y-%m",
            TimeGranularity.YEAR: "%Y",
        }[self]

    def coerce(self, value: object) -> dt.date:
        """Coerce a raw value to this granularity's value type.

        Args:
            value: A raw period label: a ``date``, a ``datetime``, or an ISO-8601 string.

        Returns:
            A ``datetime`` for the hourly granularity, a ``date`` otherwise.
        """
        if self is TimeGranularity.HOUR:
            return coerce_to_datetime(value)
        return coerce_to_date(value)

    def truncate(self, value: object) -> dt.date:
        """Reduce a value to the start of the period containing it.

        Args:
            value: Any value this granularity can coerce, at any offset inside the period.

        Returns:
            The period's first instant.
        """
        coerced = self.coerce(value)
        if self is TimeGranularity.HOUR:
            assert isinstance(coerced, dt.datetime)
            return coerced.replace(minute=0, second=0, microsecond=0)
        if self is TimeGranularity.DAY:
            return coerced
        if self is TimeGranularity.MONTH:
            return coerced.replace(day=1)
        return coerced.replace(month=1, day=1)

    def advance(self, value: object, periods: int) -> dt.date:
        """Shift a value by *periods* whole periods (negative goes back).

        Args:
            value: The starting value; truncated to its period before shifting.
            periods: How many whole periods to shift by, negative to go back.

        Returns:
            The start of the resulting period.
        """
        start = self.truncate(value)
        if self is TimeGranularity.HOUR:
            return start + dt.timedelta(hours=periods)
        if self is TimeGranularity.DAY:
            return start + dt.timedelta(days=periods)
        if self is TimeGranularity.MONTH:
            return add_months(start, periods)
        return add_months(start, 12 * periods)

    def bounds(self, value: object) -> tuple[dt.date, dt.date]:
        """Return the period's ``(start, end_exclusive)`` bounds.

        Half-open bounds are what range-scoped reads and deletes need: a partition covering more
        (or less) than one row of a ``DATE`` column cannot be selected by equality.

        Args:
            value: Any value inside the period whose bounds to return.

        Returns:
            The period's start and its exclusive end.
        """
        start = self.truncate(value)
        return start, self.advance(start, 1)

    def periods_between(self, start: object, end: object) -> int:
        """Return how many periods separate two values (``end`` minus ``start``).

        Args:
            start: The value the count starts from; truncated to its period.
            end: The value the count runs to; truncated to its period.

        Returns:
            The number of steps from *start*'s period to *end*'s period, negative when *end*
            precedes *start*.
        """
        first = self.truncate(start)
        last = self.truncate(end)
        if self is TimeGranularity.HOUR:
            assert isinstance(first, dt.datetime) and isinstance(last, dt.datetime)
            return int((last - first).total_seconds() // 3600)
        if self is TimeGranularity.DAY:
            return (last - first).days
        months = (last.year - first.year) * 12 + (last.month - first.month)
        if self is TimeGranularity.MONTH:
            return months
        return last.year - first.year

    def format(self, value: object) -> str:
        """Render a value as a partition id.

        Args:
            value: Any value inside the period the id names.

        Returns:
            The canonical id of the period containing *value*.
        """
        return self.truncate(value).strftime(self.key_format)

    def parse(self, key: str) -> dt.date:
        """Parse a partition id back into the period's start.

        Args:
            key: A partition id in this granularity's own format (see `key_format`).

        Returns:
            The period's first instant.

        Raises:
            ValueError: If *key* is not this granularity's format.
        """
        try:
            parsed = dt.datetime.strptime(key, self.key_format)
        except ValueError as error:
            raise ValueError(
                f"Partition key {key!r} is not a {self.value} key (expected the shape {self.key_format!r})."
            ) from error
        return parsed if self is TimeGranularity.HOUR else parsed.date()

    def period_range(self, start: dt.date, end: dt.date, reversed: bool = False) -> Generator[dt.date, None, None]:
        """Yield the start of each period from *start* to *end* inclusive.

        Args:
            start: Start of the range (truncated to this granularity).
            end: End of the range (truncated to this granularity).
            reversed: When True, yield periods from end to start.

        Yields:
            Each period's first instant.
        """
        first = self.truncate(start)
        last = self.truncate(end)
        if reversed:
            while last >= first:
                yield last
                last = self.advance(last, -1)
        else:
            while first <= last:
                yield first
                first = self.advance(first, 1)


# -- Time partitions -----------------------------------------------------------


@dataclass(frozen=True)
class TimePartition(Partition):
    """A single time partition, identified by the start of its period.

    Attributes:
        value: The period's start, normalised on construction.
        granularity: The period one partition covers.
    """

    value: dt.date
    granularity: TimeGranularity = TimeGranularity.DAY

    @classmethod
    def from_key(cls, key: str) -> TimePartition:
        """Parse a partition id, inferring its granularity from the shape.

        The id shapes are mutually unambiguous, so the key alone carries the granularity: storage
        and transport need one string, not a pair.

        Args:
            key: A partition id in any granularity's format.

        Returns:
            The partition the key names.

        Raises:
            PartitionError: If *key* matches no known id shape.
        """
        for granularity in TimeGranularity:
            try:
                return cls(granularity.parse(key), granularity)
            except ValueError:
                continue
        shapes = ", ".join(repr(granularity.key_format) for granularity in TimeGranularity)
        raise PartitionError(f"Partition key {key!r} matches no granularity (known shapes: {shapes}).")

    def __post_init__(self) -> None:
        """Normalise the value to the start of its period."""
        object.__setattr__(self, "value", self.granularity.truncate(self.value))

    @property
    def id(self) -> str:
        """The partition's canonical id (see `TimeGranularity.format`)."""
        return self.granularity.format(self.value)

    @property
    def bounds(self) -> tuple[dt.date, dt.date]:
        """The partition's ``(start, end_exclusive)`` bounds."""
        return self.granularity.bounds(self.value)

    @property
    def window(self) -> TimePartitionWindow:
        """The one-partition window covering only this period."""
        return TimePartitionWindow(self.value, self.value, self.granularity)

    def filter(self, column: str) -> PartitionFilter:
        """The rows inside this period.

        Rows may carry values anywhere inside the period (a monthly partition whose rows hold
        daily dates), so the filter is the half-open bounds, never the id.

        Args:
            column: The partition column the rows carry.

        Returns:
            A bounds filter on the period.
        """
        return PartitionFilter(column, bounds=self.bounds)

    def slice(self, data: Any, column: str) -> Any:
        """Return this partition's slice of *data*, selecting by its half-open bounds.

        Args:
            data: The table to slice, in any registered representation's type.
            column: Name of the partition column to select on.

        Returns:
            The partition's slice of the data.
        """
        from interloper.representation import Representation

        return Representation.of(data).filter_range(column, *self.bounds)


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    """A contiguous range of time partitions, inclusive of both bounds.

    Attributes:
        start: The first period's start.
        end: The last period's start.
        granularity: The period one partition covers.
    """

    start: dt.date
    end: dt.date
    granularity: TimeGranularity = TimeGranularity.DAY

    def __post_init__(self) -> None:
        """Normalise both bounds to period starts.

        Raises:
            ValueError: If the window ends before it starts.
        """
        object.__setattr__(self, "start", self.granularity.truncate(self.start))
        object.__setattr__(self, "end", self.granularity.truncate(self.end))
        if self.start > self.end:
            raise ValueError(f"Time partition window ends before it starts: {self.start} to {self.end}.")

    @property
    def first(self) -> TimePartition:
        """The window's first partition."""
        return TimePartition(self.start, self.granularity)

    @property
    def last(self) -> TimePartition:
        """The window's last partition."""
        return TimePartition(self.end, self.granularity)

    @property
    def partitions(self) -> list[Partition]:
        """Every period from ``start`` to ``end`` inclusive, oldest first."""
        return [TimePartition(value, self.granularity) for value in self.granularity.period_range(self.start, self.end)]

    @property
    def bounds(self) -> tuple[dt.date, dt.date]:
        """The window's ``(start, end_exclusive)`` bounds: the first period's start to the last's end."""
        return self.start, self.granularity.advance(self.end, 1)

    def __len__(self) -> int:
        """Count the window's partitions.

        Returns:
            The number of periods from ``start`` to ``end`` inclusive, at least 1.
        """
        return self.granularity.periods_between(self.start, self.end) + 1

    def filter(self, column: str) -> PartitionFilter:
        """The rows inside the whole window.

        Args:
            column: The partition column the rows carry.

        Returns:
            A bounds filter spanning every period of the window.
        """
        return PartitionFilter(column, bounds=self.bounds)

    @classmethod
    def lookback(
        cls,
        now: dt.date,
        lookback: int,
        offset: int = 1,
        granularity: TimeGranularity = TimeGranularity.DAY,
        start: dt.date | None = None,
    ) -> TimePartitionWindow | None:
        """Build the trailing window a scheduled workload should cover.

        The window is counted in partitions, never in days: *offset* is how many partitions back
        from the current one the window ends, and *lookback* is how many it spans. At daily
        granularity, ``offset=1, lookback=1`` is "yesterday only" and ``offset=0`` includes the
        current (still incomplete) partition.

        The caller's timezone reaches the window through *now*: passing an aware datetime
        localized to a zone makes DAY, MONTH and YEAR windows follow that zone's calendar. HOUR
        windows are the deliberate exception: an aware *now* is normalized back to UTC (see
        `coerce_to_datetime`), because hour partition ids are naive-UTC labels and zones on a
        fractional offset do not align to UTC hour boundaries at all.

        Args:
            now: The reference instant, in whatever timezone the caller labels partitions with.
            lookback: How many partitions the window spans (at least 1).
            offset: How many partitions back from the current one the window ends.
            granularity: The period one partition covers.
            start: Optional lower bound (an asset's earliest partition) to clamp the window to.

        Returns:
            The window, or ``None`` if clamping to *start* leaves it empty.

        Raises:
            ValueError: If *lookback* is below 1, or *offset* is negative.
        """
        if lookback < 1:
            raise ValueError(f"lookback must cover at least one partition, got {lookback}.")
        if offset < 0:
            raise ValueError(f"offset cannot be negative, got {offset}.")

        end = granularity.advance(now, -offset)
        first = granularity.advance(end, -(lookback - 1))

        if start is not None:
            bound = granularity.truncate(start)
            if end < bound:
                return None
            first = max(first, bound)

        return cls(first, end, granularity)


@dataclass(frozen=True, kw_only=True)
class TimePartitionConfig(PartitionConfig):
    """Partitioning by time periods of one granularity.

    Attributes:
        granularity: The period one partition covers.
        start: Optional lower bound. Windows reaching before it are rejected, and trailing
            windows built against this asset are clamped to it.
    """

    kind: ClassVar[str] = "time"

    granularity: TimeGranularity = TimeGranularity.DAY
    start: dt.date | None = None

    def __post_init__(self) -> None:
        """Coerce what a definition round trip loosens: the granularity name and the start."""
        object.__setattr__(self, "granularity", TimeGranularity(self.granularity))
        if self.start is not None:
            object.__setattr__(self, "start", self.granularity.truncate(self.start))

    def _check(self, window: PartitionWindow) -> None:
        """Require a time window of this granularity that does not reach before ``start``.

        Args:
            window: The run's window.

        Raises:
            PartitionError: If the window is not a time window, has another granularity, or
                starts before ``start``.
        """
        if not isinstance(window, TimePartitionWindow):
            raise PartitionError(f"expects a time window, but the run was given a {type(window).__name__}")
        if window.granularity is not self.granularity:
            raise PartitionError(
                f"is partitioned by {self.granularity.value}, but the run was given a {window.granularity.value} window"
            )
        if self.start is not None and window.start < self.start:
            raise PartitionError(
                f"has no data before {self.start.isoformat()}, but the run reaches back to {window.start.isoformat()}"
            )

    def partition(self, key: str) -> TimePartition:
        """Parse a key at this granularity.

        Args:
            key: The partition key.

        Returns:
            The partition.

        Raises:
            PartitionError: If the key is not of this granularity's shape.
        """
        try:
            return TimePartition(self.granularity.parse(key), self.granularity)
        except ValueError as error:
            raise PartitionError(str(error)) from None

    def _window(self, first: Partition, last: Partition) -> TimePartitionWindow:
        """Build the window between two periods.

        Args:
            first: The first partition.
            last: The last partition.

        Returns:
            The window.

        Raises:
            PartitionError: If *last* precedes *first*.
        """
        assert isinstance(first, TimePartition) and isinstance(last, TimePartition)
        if last.value < first.value:
            raise PartitionError(f"window ends before it starts: {first.id!r} to {last.id!r}")
        return TimePartitionWindow(first.value, last.value, self.granularity)

    def compatible(self, other: PartitionConfig) -> bool:
        """Whether *other* is a time partitioning of the same granularity.

        Args:
            other: The partitioning at the other end of a DAG edge.

        Returns:
            True when both share the granularity.
        """
        return isinstance(other, TimePartitionConfig) and other.granularity is self.granularity
```

- [ ] **Step 7: Export the new names**

`partitioning/__init__.py`:

```python
"""Partitioning: the ordered space an asset is split into, the window a run covers, and the two kinds."""

from interloper.partitioning.base import Partition, PartitionConfig, PartitionFilter, PartitionWindow
from interloper.partitioning.list import ListPartition, ListPartitionConfig, ListPartitionWindow
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig, TimePartitionWindow

__all__ = [
    "ListPartition",
    "ListPartitionConfig",
    "ListPartitionWindow",
    "Partition",
    "PartitionConfig",
    "PartitionFilter",
    "PartitionWindow",
    "TimeGranularity",
    "TimePartition",
    "TimePartitionConfig",
    "TimePartitionWindow",
]
```

In `interloper/__init__.py`, add `ListPartition`, `ListPartitionConfig`, `ListPartitionWindow` to the `from interloper.partitioning import (...)` block and to `__all__` (alphabetical).

In `destination/__init__.py`, change the database import to `from interloper.destination.database import DatabaseDestination` and add `from interloper.partitioning import PartitionFilter`; keep `"PartitionFilter"` in `__all__` so the backends' `from interloper.destination import PartitionFilter` keeps working.

- [ ] **Step 8: Move `PartitionFilter` out of `destination/database.py` and render filters through the partition**

In `database.py`: delete the `PartitionFilter` dataclass and the `TimePartition` import; add `from interloper.partitioning import PartitionFilter`; replace `_filter` with:

```python
    def _filter(self, context: IOContext, partition: Partition | None) -> PartitionFilter | None:
        """The rows one partition covers, as the partition renders them.

        Args:
            context: IO context whose asset supplies the partition column.
            partition: The partition, or ``None`` for the whole table.

        Returns:
            The filter, or ``None`` for the whole table.
        """
        if partition is None:
            return None
        return partition.filter(self._partition_column(context))
```

Update the `DatabaseDestination` class docstring sentence "(half-open bounds for a time partition, equality otherwise)" to "(as the partition renders it: bounds for a time partition, membership for a list one)".

- [ ] **Step 9: `Asset.definition` renders the partitioning through `to_definition`**

In `asset/base.py`, replace `partitioning = asdict(cls.partitioning) if cls.partitioning is not None else None` with `partitioning = cls.partitioning.to_definition() if cls.partitioning is not None else None` and drop the `from dataclasses import asdict` import.

- [ ] **Step 10: Rewrite the time tests that pinned removed behaviour**

In `tests/partitioning/test_time.py`:
- Delete `test_undeclarable_granularities_have_no_id_format`, `test_supported_set_is_bigquery_parity`, `test_unsupported_granularity_is_rejected`, every parametrised case naming `WEEK` or `QUARTER`, `test_iterates_newest_first`, `test_partition_count_is_inclusive`, `test_str_and_repr_are_iso`, and `test_rejects_an_unknown_shape`'s `ConfigError` import (it now expects `PartitionError`, which is a `ConfigError`).
- Add to `TestTimePartition`:

```python
    def test_str_is_the_id(self) -> None:
        assert str(TimePartition(dt.date(2026, 5, 20), TimeGranularity.MONTH)) == "2026-05"

    def test_filter_is_the_half_open_bounds(self) -> None:
        partition = TimePartition(dt.date(2026, 5, 20), TimeGranularity.MONTH)
        assert partition.filter("date") == PartitionFilter("date", bounds=(dt.date(2026, 5, 1), dt.date(2026, 6, 1)))

    def test_window_is_the_one_period_window(self) -> None:
        partition = TimePartition(dt.date(2026, 5, 20), TimeGranularity.MONTH)
        assert partition.window == TimePartitionWindow(dt.date(2026, 5, 1), dt.date(2026, 5, 1), TimeGranularity.MONTH)
```

- Add to `TestTimePartitionWindow`:

```python
    def test_partitions_are_oldest_first(self) -> None:
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        assert [partition.id for partition in window] == ["2026-01-01", "2026-01-02", "2026-01-03"]
        assert window.first.id == "2026-01-01" and window.last.id == "2026-01-03"

    def test_len_is_inclusive(self) -> None:
        assert len(TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))) == 3
        assert len(TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 1))) == 1

    def test_bounds_and_filter_span_the_whole_window(self) -> None:
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        assert window.bounds == (dt.date(2026, 1, 1), dt.date(2026, 1, 4))
        assert window.filter("date") == PartitionFilter("date", bounds=(dt.date(2026, 1, 1), dt.date(2026, 1, 4)))

    def test_str_uses_partition_ids(self) -> None:
        window = TimePartitionWindow(dt.datetime(2026, 1, 1, 13), dt.datetime(2026, 1, 1, 15), TimeGranularity.HOUR)
        assert str(window) == "2026-01-01T13..2026-01-01T15"
```

- Add a class `TestTimePartitionConfigContract`:

```python
class TestTimePartitionConfigContract:
    def test_check_accepts_its_own_granularity(self) -> None:
        TimePartitionConfig(column="date").check(TimePartition(dt.date(2026, 1, 1)).window)

    def test_check_rejects_another_granularity(self) -> None:
        with pytest.raises(PartitionError, match="partitioned by day, but the run was given a month window"):
            TimePartitionConfig(column="date").check(TimePartition(dt.date(2026, 1, 1), TimeGranularity.MONTH).window)

    def test_check_rejects_a_window_before_the_start(self) -> None:
        config = TimePartitionConfig(column="date", allow_window=True, start=dt.date(2026, 1, 10))
        with pytest.raises(PartitionError, match="no data before 2026-01-10"):
            config.check(TimePartitionWindow(dt.date(2026, 1, 5), dt.date(2026, 1, 20)))
        config.check(TimePartitionWindow(dt.date(2026, 1, 10), dt.date(2026, 1, 20)))

    def test_check_rejects_a_list_window(self) -> None:
        from interloper.partitioning.list import ListPartition

        with pytest.raises(PartitionError, match="expects a time window"):
            TimePartitionConfig(column="date").check(ListPartition("eu").window)

    def test_partition_parses_at_its_granularity(self) -> None:
        config = TimePartitionConfig(column="date", granularity=TimeGranularity.MONTH)
        assert config.partition("2026-05") == TimePartition(dt.date(2026, 5, 1), TimeGranularity.MONTH)
        with pytest.raises(PartitionError, match="not a month key"):
            config.partition("2026-05-01")

    def test_window_rejects_an_inverted_range(self) -> None:
        with pytest.raises(PartitionError, match="before it starts"):
            TimePartitionConfig(column="date").window("2026-01-03", "2026-01-01")

    def test_compatible_means_the_same_granularity(self) -> None:
        daily = TimePartitionConfig(column="date")
        assert daily.compatible(TimePartitionConfig(column="day")) is True
        assert daily.compatible(TimePartitionConfig(column="date", granularity=TimeGranularity.MONTH)) is False

    def test_granularity_name_is_coerced(self) -> None:
        assert TimePartitionConfig(column="date", granularity="month").granularity is TimeGranularity.MONTH  # ty: ignore[invalid-argument-type]
```

Add `from interloper.errors import PartitionError` and `from interloper.partitioning.base import PartitionFilter` to the imports; `TestTimePartitionFromKey::test_rejects_an_unknown_shape` now expects `PartitionError`.

- [ ] **Step 11: Replace the generic partition fakes in the other core tests**

`tests/destination/test_database.py`:
- Import `from interloper.partitioning import ListPartition, ListPartitionConfig, PartitionFilter` and drop the `PartitionFilter` import from `interloper.destination.database`.
- In `test_non_time_partition_deletes_by_id` and `test_any_other_partition_filters_by_its_id`: replace the local imports and the asset with `@il.asset(partitioning=ListPartitionConfig(column="region", values=("eu", "us")))`, `Partition("eu")` with `ListPartition("eu")`, and the expectations with `PartitionFilter("region", values=("eu",))`.

`tests/asset/test_base.py`:
- Replace the imports of `Partition, PartitionConfig, PartitionWindow` with `from interloper.partitioning import ListPartition, ListPartitionConfig, ListPartitionWindow`; delete `FakePartition`, `FakePartitionWindow`, the `Iterator` and `dataclass` imports if nothing else uses them.
- `FakeAssetPartitioned.partitioning = ListPartitionConfig(column="day", values=("x", "a", "b"))`, `FakeAssetPartitionedWithWindow` the same with `allow_window=True`; line 286's `PartitionConfig(column="day")` becomes the same list config.
- In `TestPartitioning`: `FakePartition(value="x")` becomes `ListPartition("x")`, `FakePartitionWindow(start="a", end="b")` becomes `ListPartitionWindow("a", "b", ("x", "a", "b"))`.
- In `TestTimePartitioning` and `TestTimePartitionScope`: `FakePartition(value="2026-01-01")` and `FakePartition("x")` become `ListPartition("x")` with the match text `"is time-partitioned, but the run was given a ListPartition"`; `FakePartitionWindow(start="2026-01-01", end="2026-01-03")` becomes `ListPartitionWindow("a", "b", ("a", "b"))`.

`tests/asset/test_context.py`: replace `PartitionConfig(column="region")` with `ListPartitionConfig(column="region", values=("eu",))` and `Partition("eu")` with `ListPartition("eu")`, importing both from `interloper.partitioning`.

`tests/runner/test_results.py`: in `test_names_the_window`, expect `"window=2026-06-01..2026-06-03"`.

- [ ] **Step 12: Run the core suite, ruff and ty**

Run: `uv run pytest packages/interloper-core -q && uv run ruff check packages/interloper-core && uv run ty check packages/interloper-core`
Expected: all green. `interloper-db` and the destination packages are expected to break until Task 7; do not touch them here.

---

### Task 2: Validation through the config

**Files:**
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (`run_async`, delete `_validate_partitioning` and `_validate_time_partitioning`, add `_check_window`)
- Modify: `packages/interloper-core/src/interloper/operation/base.py` (delete `_validate_time_partitioning`)
- Modify: `packages/interloper-core/src/interloper/runner/base.py` (`_preflight`)
- Modify: `packages/interloper-core/src/interloper/dag/base.py` (`_check_partition_dependencies`)
- Modify: `packages/interloper-core/tests/asset/test_base.py`, `tests/runner/test_base.py`, `tests/dag/test_base.py`, `tests/cli/commands/test_run.py`

**Interfaces:**
- Consumes: `PartitionConfig.check(window)`, `PartitionConfig.compatible(other)`, `PartitionWindow.of(scope)` from Task 1.
- Produces: `Asset._check_window(window: PartitionWindow | None) -> None` raising `PartitionError("Asset '<key>' <predicate>")`; `Runner._preflight(dag, window: PartitionWindow | None) -> None` raising one `PartitionError` listing every failing operation; `DAG` rejects incompatible partitionings along an edge with `DAGError(... "are partitioned differently" ...)`.

- [ ] **Step 1: Write the failing asset tests**

In `tests/asset/test_base.py`, delete the classes `TestPartitioning`, `TestTimePartitioning`, `TestTimePartitionScope` and the fakes `FakeAssetPartitioned`, `FakeAssetPartitionedWithWindow` (keep `FakeAssetDaily` and `FakeAssetBounded`). Add, where `TestPartitioning` was:

```python
class FakeAssetRegional(il.Asset):
    partitioning = ListPartitionConfig(column="region", values=("eu", "us"))


class TestWindowValidation:
    """The asset defers to its partitioning's check; an unpartitioned asset ignores the window."""

    def test_an_unpartitioned_asset_ignores_the_window(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            FakeAsset()._check_window(TimePartition(dt.date(2026, 1, 1)).window)
            FakeAsset()._check_window(None)

    def test_a_partitioned_asset_needs_a_window(self):
        with pytest.raises(PartitionError, match="Asset 'fake_asset_daily' is partitioned, but the run was given no window"):
            FakeAssetDaily()._check_window(None)

    def test_the_message_names_the_asset_and_the_predicate(self):
        with pytest.raises(PartitionError, match="Asset 'fake_asset_bounded' has no data before 2026-01-10"):
            FakeAssetBounded()._check_window(TimePartition(dt.date(2026, 1, 9)).window)

    def test_a_list_asset_accepts_a_declared_partition(self):
        FakeAssetRegional()._check_window(ListPartition("eu").window)
        with pytest.raises(PartitionError, match="'apac' is not one of the declared partitions"):
            FakeAssetRegional()._check_window(ListPartition("apac").window)

    def test_run_checks_before_anything_executes(self):
        calls: list[str] = []

        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def daily(context: il.ExecutionContext) -> list:
            calls.append("ran")
            return []

        with pytest.raises(PartitionError, match="given no window"):
            daily().run()
        assert calls == []

    def test_run_accepts_a_bare_partition_as_sugar(self):
        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def daily(context: il.ExecutionContext) -> list:
            return [{"date": context.partition_date}]

        assert daily().run(TimePartition(dt.date(2026, 1, 1))) == [{"date": dt.date(2026, 1, 1)}]
```

Add `import warnings` to the test module's imports.

- [ ] **Step 2: Write the failing runner, DAG and CLI tests**

`tests/runner/test_base.py`, class `TestPreflightValidation`:
- `test_partitioned_assets_require_a_partition`: match `r"not valid for every operation: 'daily' is partitioned, but the run was given no window"`.
- `test_scope_before_the_asset_start_fails_the_run`: match `"'bounded' has no data before 2026-01-10"`.
- `test_mismatched_granularity_fails_the_run`: match `"'daily' is partitioned by day, but the run was given a month window"`.
- `test_a_windowed_run_needs_every_asset_to_allow_windows`: match `r"'daily_only' does not allow windows"`.
- `test_non_enabled_partitioned_assets_are_exempt`: call `AsyncRunner()._preflight(il.DAG(asset), None)`.
- Add:

```python
    async def test_every_failing_operation_is_listed(self):
        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def one() -> list[dict[str, Any]]:
            return []

        @il.asset(partitioning=TimePartitionConfig(column="date"))
        def two() -> list[dict[str, Any]]:
            return []

        dag = il.DAG(one(id="one", destinations=[il.MemoryDestination()]), two(id="two", destinations=[il.MemoryDestination()]))
        with pytest.raises(PartitionError, match=r"'one' is partitioned.*; 'two' is partitioned"):
            await AsyncRunner().run(dag)
```

`tests/dag/test_base.py`, `test_mixed_granularity_raises_even_for_read_only_upstreams`: match `"partitioned differently"`. Add to `TestGranularityAcrossEdges`:

```python
    def test_a_list_upstream_of_a_time_dependent_is_rejected(self):
        class Regions(il.Asset):
            partitioning = il.ListPartitionConfig(column="region", values=("eu",))

        class DailyOverRegions(il.Asset):
            partitioning = il.TimePartitionConfig(column="date")
            regions: Regions = il.Relation("asset", "regions")

            def data(self, regions: il.Upstream) -> Any:  # pragma: no cover
                return None

        regions = Regions()
        with pytest.raises(DAGError, match="partitioned differently"):
            DAG(regions, DailyOverRegions(regions=regions))
```

`tests/cli/commands/test_run.py`, class `TestRunPreflightErrors`:
- `test_window_without_allow_window_is_a_clean_error`: `match="Error: The run's window is not valid"` and assert `"does not allow windows" in str(exc_info.value)`.
- `test_missing_partition_is_a_clean_error`: `match="given no window"`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/asset/test_base.py::TestWindowValidation packages/interloper-core/tests/runner/test_base.py::TestPreflightValidation packages/interloper-core/tests/dag/test_base.py::TestGranularityAcrossEdges -q`
Expected: FAIL (`_check_window` and `_preflight` undefined, old messages).

- [ ] **Step 4: Replace the asset validators**

In `asset/base.py`: in `run_async`, replace `self._validate_partitioning(partition_or_window)` with `self._check_window(PartitionWindow.of(partition_or_window))`. Delete `_validate_partitioning` and `_validate_time_partitioning`. Add, in the `# -- Internals` section:

```python
    def _check_window(self, window: PartitionWindow | None) -> None:
        """Check a run's window against this asset's partitioning.

        An unpartitioned asset accepts any window and ignores it; a partitioned one defers to
        its config, whose predicate is prefixed with the asset's name.

        Args:
            window: The run's window, ``None`` when the run was unscoped.

        Raises:
            PartitionError: If the window is not valid for the asset's partitioning.
        """
        if self.partitioning is None:
            return
        try:
            self.partitioning.check(window)
        except PartitionError as error:
            raise PartitionError(f"Asset '{self.key}' {error}") from None
```

Drop the now-unused imports (`TimePartition`, `TimePartitionConfig`, `TimePartitionWindow`); keep `warnings` for the module-level filter.

In `operation/base.py`, delete `_validate_time_partitioning` and its `Any` import if unused.

- [ ] **Step 5: Replace the runner preflight**

In `runner/base.py`, replace `_preflight_validation` with:

```python
    def _preflight(self, dag: DAG, window: PartitionWindow | None) -> None:
        """Check the run's window against every enabled partitioned operation before anything executes.

        A run whose window no operation can serve fails as a whole, listing every failing
        operation, instead of failing the offending operations while the rest of the DAG
        executes.

        Args:
            dag: The DAG whose operations are checked.
            window: The run's window, ``None`` for an unscoped run.

        Raises:
            PartitionError: If the window is not valid for at least one enabled partitioned
                operation, naming each one with its own predicate.
        """
        problems: list[str] = []
        for operation in dag.operations:
            if not operation.enabled or operation.partitioning is None:
                continue
            try:
                operation.partitioning.check(window)
            except PartitionError as error:
                problems.append(f"'{operation.key}' {error}")
        if problems:
            raise PartitionError("The run's window is not valid for every operation: " + "; ".join(problems))
```

In `_init_run`, call `self._preflight(dag, PartitionWindow.of(partition_or_window))`. Import `PartitionWindow` from `interloper.partitioning.base`; drop the `TimePartitionConfig` import.

- [ ] **Step 6: Generalise the DAG edge rule**

In `dag/base.py`, `_check_partition_dependencies`, replace the granularity `if` with:

```python
                if (
                    upstream.partitioning is not None
                    and operation.partitioning is not None
                    and not upstream.partitioning.compatible(operation.partitioning)
                ):
                    raise DAGError(
                        f"Invalid upstream: '{upstream.key}' and its dependent '{operation.key}' are partitioned "
                        "differently; a run has one window, so both ends of an edge must accept the same windows."
                    )
```

Update the method docstring ("equal granularity between time-partitioned ends" becomes "compatible partitionings at both ends"). Drop the `TimePartitionConfig` import.

- [ ] **Step 7: Run the core suite, ruff and ty**

Run: `uv run pytest packages/interloper-core -q && uv run ruff check packages/interloper-core && uv run ty check packages/interloper-core`
Expected: green.

---

### Task 3: Contexts and the window rename

**Files:**
- Rewrite: `packages/interloper-core/src/interloper/asset/context.py`
- Rewrite: `packages/interloper-core/src/interloper/destination/context.py`
- Modify: `packages/interloper-core/src/interloper/operation/base.py` (`OperationContext.window`, `effective_window`, `_event_metadata` parameter)
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (every `partition_or_window` becomes `window`; `ExecutionContext(self, window, metadata)`; `IOContext(window=...)`)
- Modify: `packages/interloper-core/src/interloper/runner/base.py`, `state.py`, `results.py`, `async_runner.py`, `sync_runner.py`, `multi_process.py`, `dag/base.py` (the rename)
- Modify: `packages/interloper-core/src/interloper/destination/database.py` (`context.window` was a bool)
- Modify: tests under `packages/interloper-core/tests/` (the rename, the two context test modules)

**Interfaces:**
- Produces: `OperationContext(window, dag, metadata)`; `ExecutionContext(asset, window=None, metadata=None)` with `asset`, `window`, `metadata`, `partition`, `partition_date`, `logger`; `IOContext(asset, window=None, metadata=..., schema=None)` with `partitions`, `slices(data)`; `Operation.effective_window(window)`; `Asset.run/run_async/materialize/materialize_async(window=None, dag=None, metadata=None)`; `Runner.run(dag, window=None, metadata=None)`; `RunState.start_run(window)`, `RunState.window`; `RunResult.window`; `DAG.materialize(window=None)`.

- [ ] **Step 1: Write the failing context tests**

Replace `tests/asset/test_context.py` with:

```python
"""Tests for ``interloper.asset.context``."""

from __future__ import annotations

import datetime as dt

import pytest

import interloper as il
from interloper.asset.context import ExecutionContext
from interloper.partitioning import (
    ListPartition,
    ListPartitionConfig,
    TimeGranularity,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)


@il.asset(partitioning=TimePartitionConfig(column="date"))
def daily(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=TimePartitionConfig(column="date", allow_window=True))
def windowed(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=TimePartitionConfig(column="date", granularity=TimeGranularity.MONTH))
def monthly(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=ListPartitionConfig(column="region", values=("eu", "us")))
def regional(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def whole() -> list:  # noqa: D103
    return []


JAN_1 = TimePartition(dt.date(2026, 1, 1))
JAN_1_TO_3 = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))


class TestAssetAndMetadata:
    def test_hands_back_the_asset(self) -> None:
        asset = daily()
        assert ExecutionContext(asset).asset is asset

    def test_metadata_defaults_to_empty(self) -> None:
        assert ExecutionContext(daily()).metadata == {}

    def test_metadata_is_handed_back_as_given(self) -> None:
        assert ExecutionContext(daily(), metadata={"run_id": "r1"}).metadata == {"run_id": "r1"}


class TestWindow:
    def test_hands_back_the_window(self) -> None:
        assert ExecutionContext(windowed(), JAN_1_TO_3).window is JAN_1_TO_3

    def test_a_window_of_one_is_readable_without_allow_window(self) -> None:
        # The config's check already bounds a non-windowed asset to one partition.
        window = ExecutionContext(daily(), JAN_1.window).window
        assert isinstance(window, TimePartitionWindow)
        assert (window.start, window.end) == (dt.date(2026, 1, 1), dt.date(2026, 1, 1))
        assert window.granularity is TimeGranularity.DAY

    def test_raises_when_unpartitioned(self) -> None:
        with pytest.raises(AttributeError, match="`context.window` is not available, asset is not partitioned"):
            ExecutionContext(whole(), JAN_1.window).window

    def test_raises_without_a_window(self) -> None:
        with pytest.raises(AttributeError, match="`context.window` is not available, no window provided"):
            ExecutionContext(daily()).window


class TestPartition:
    def test_hands_back_the_only_partition(self) -> None:
        assert ExecutionContext(daily(), JAN_1.window).partition == JAN_1

    def test_the_partition_answers_its_own_scope(self) -> None:
        partition = ExecutionContext(daily(), JAN_1.window).partition
        assert isinstance(partition, TimePartition)
        assert partition.id == "2026-01-01"
        assert partition.bounds == (dt.date(2026, 1, 1), dt.date(2026, 1, 2))

    def test_works_for_a_list_partition(self) -> None:
        assert ExecutionContext(regional(), ListPartition("eu").window).partition.id == "eu"

    def test_raises_when_unpartitioned(self) -> None:
        with pytest.raises(AttributeError, match="asset is not partitioned"):
            ExecutionContext(whole()).partition

    def test_raises_without_a_window(self) -> None:
        with pytest.raises(AttributeError, match="no window provided"):
            ExecutionContext(daily()).partition

    def test_raises_on_several_partitions(self) -> None:
        with pytest.raises(AttributeError, match="`context.partition` is not available, the run covers 3 partitions"):
            ExecutionContext(windowed(), JAN_1_TO_3).partition


class TestPartitionDate:
    def test_returns_the_day(self) -> None:
        assert ExecutionContext(daily(), JAN_1.window).partition_date == dt.date(2026, 1, 1)

    def test_a_monthly_asset_is_pointed_at_partition_instead(self) -> None:
        context = ExecutionContext(monthly(), TimePartition(dt.date(2026, 6, 1), TimeGranularity.MONTH).window)
        with pytest.raises(AttributeError, match=r"partitioned by month\. Use `context\.partition`"):
            context.partition_date

    def test_a_list_asset_is_not_time_partitioned(self) -> None:
        with pytest.raises(AttributeError, match="asset is not time-partitioned"):
            ExecutionContext(regional(), ListPartition("eu").window).partition_date

    def test_raises_on_several_partitions(self) -> None:
        with pytest.raises(AttributeError, match="covers 3 partitions"):
            ExecutionContext(windowed(), JAN_1_TO_3).partition_date

    def test_an_unpartitioned_asset_names_the_accessor(self) -> None:
        with pytest.raises(AttributeError, match="`context.partition_date` is not available, asset is not partitioned"):
            ExecutionContext(whole()).partition_date


class TestLogger:
    def test_it_is_built_once_and_reused(self) -> None:
        context = ExecutionContext(daily())
        assert context.logger is context.logger

    def test_messages_become_log_events_attributed_to_the_asset(self) -> None:
        from interloper.events import Event, EventBus, EventType

        asset = daily(id="a1")
        context = ExecutionContext(asset, JAN_1.window, metadata={"run_id": "r1"})
        captured: list[Event] = []
        EventBus.subscribe(captured.append)
        try:
            context.logger.info("fetching page 1")
            EventBus.flush(timeout=5.0)
        finally:
            EventBus.unsubscribe(captured.append)

        logs = [event for event in captured if event.type is EventType.LOG]
        assert len(logs) == 1
        assert logs[0].metadata["message"] == "fetching page 1"
        assert logs[0].metadata["component_id"] == "a1"
        assert logs[0].metadata["component_key"] == "daily"
        assert logs[0].metadata["run_id"] == "r1"
```

Replace `tests/destination/test_context.py` with:

```python
"""Tests for ``interloper.destination.context``: the partitions an IO context spells out."""

import datetime

import pytest

import interloper as il
from interloper.destination import IOContext
from interloper.errors import ConfigError
from interloper.partitioning import ListPartition, ListPartitionConfig, TimePartition, TimePartitionWindow


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def daily(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset(partitioning=ListPartitionConfig(column="region", values=("eu", "us", "apac")))
def regional(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


@il.asset
def whole() -> list:  # noqa: D103
    return []


class TestPartitions:
    def test_the_unpartitioned_whole_is_one_partition_none(self):
        assert IOContext(asset=whole()).partitions == [None]

    def test_a_window_of_one_is_its_partition(self):
        partition = TimePartition(datetime.date(2026, 9, 7))
        assert IOContext(asset=daily(), window=partition.window).partitions == [partition]

    def test_a_window_is_its_partitions_oldest_first(self):
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 9))
        context = IOContext(asset=daily(), window=window)
        assert [p.id if p else None for p in context.partitions] == ["2026-09-07", "2026-09-08", "2026-09-09"]


class TestSlices:
    def test_a_single_partition_receives_the_data_whole(self):
        rows = [{"date": "2026-09-07"}, {"date": "2026-09-08"}]
        assert IOContext(asset=whole()).slices(rows) == [(None, rows)]
        partition = TimePartition(datetime.date(2026, 9, 7))
        assert IOContext(asset=daily(), window=partition.window).slices(rows) == [(partition, rows)]

    def test_a_window_slices_on_the_partition_column(self):
        rows = [{"date": "2026-09-07", "v": 1}, {"date": "2026-09-08", "v": 2}, {"date": "2026-09-08", "v": 3}]
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        pairs = IOContext(asset=daily(), window=window).slices(rows)
        sliced = {p.id if p else None: chunk for p, chunk in pairs}
        assert sliced == {"2026-09-07": [rows[0]], "2026-09-08": rows[1:]}

    def test_a_list_window_slices_by_value(self):
        rows = [{"region": "eu", "v": 1}, {"region": "us", "v": 2}, {"region": "apac", "v": 3}]
        window = regional().partitioning.window("eu", "us")  # ty: ignore[possibly-unbound-attribute]
        pairs = IOContext(asset=regional(), window=window).slices(rows)
        assert [(p.id if p else None, chunk) for p, chunk in pairs] == [("eu", [rows[0]]), ("us", [rows[1]])]

    def test_a_window_over_an_unpartitioned_asset_is_a_config_error(self):
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        with pytest.raises(ConfigError, match="not partitioned"):
            IOContext(asset=whole(), window=window).slices([])


def test_a_list_partition_keys_memory_storage_by_its_id() -> None:
    """The built-in destinations key storage by ``partition.id``, which a list partition provides."""
    il.MemoryDestination.clear()
    destination = il.MemoryDestination()
    context = IOContext(asset=regional(), window=ListPartition("eu").window)
    destination.write(context, [{"region": "eu", "v": 1}])
    assert destination.read(context) == [{"region": "eu", "v": 1}]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/asset/test_context.py packages/interloper-core/tests/destination/test_context.py -q`
Expected: FAIL (`ExecutionContext.__init__` takes `asset_key`, `IOContext` has no `window` field).

- [ ] **Step 3: Write `asset/context.py`**

```python
"""Runtime context for asset execution."""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

from interloper.events import EventLogger
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimeGranularity, TimePartition, TimePartitionConfig

if TYPE_CHECKING:
    from interloper.asset.base import Asset


class ExecutionContext:
    """What an asset's ``data()`` knows about the run it executes in.

    Built by the asset for each run from itself, the run's window and the run metadata, and
    handed to ``data()`` as its ``context`` parameter. Each partition accessor raises an
    ``AttributeError`` naming why it is unavailable: the asset is not partitioned, no window was
    given, the window holds several partitions, or the granularity is not daily.
    """

    def __init__(
        self,
        asset: Asset,
        window: PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Bind the context to one run of one asset.

        Args:
            asset: The asset executing.
            window: The window the run covers, ``None`` for an unpartitioned asset.
            metadata: Run metadata (run id, backfill id), carried onto every event the asset
                emits; ``None`` is an empty dict.
        """
        self._asset = asset
        self._window = window
        self._metadata = metadata or {}
        self._logger: EventLogger | None = None

    @property
    def asset(self) -> Asset:
        """The asset executing: its key, table, dataset, source and partitioning."""
        return self._asset

    @property
    def metadata(self) -> dict[str, Any]:
        """The run metadata (run id, backfill id, anything the caller passed)."""
        return self._metadata

    @property
    def logger(self) -> EventLogger:
        """Logger whose messages become ``LOG`` events attributed to the asset."""
        if self._logger is None:
            parent = self._asset.parent
            self._logger = EventLogger(
                self._asset.key,
                self._metadata,
                component_id=self._asset.id,
                parent_id=parent.id if parent is not None else None,
            )
        return self._logger

    @property
    def window(self) -> PartitionWindow:
        """The window this run covers, of one partition unless the asset allows windows.

        Raises:
            AttributeError: If the asset is not partitioned, or no window was given.
        """
        return self._require_window("window")

    @property
    def partition(self) -> Partition:
        """The partition this run covers: the window's only one.

        The partition answers everything about its own scope: ``id``, ``bounds`` and
        ``granularity`` for a time partition, ``value`` for a list one.

        Raises:
            AttributeError: If the asset is not partitioned, no window was given, or the window
                covers several partitions.
        """
        window = self._require_window("partition")
        if len(window) > 1:
            raise AttributeError(f"`context.partition` is not available, the run covers {len(window)} partitions.")
        return window.first

    @property
    def partition_date(self) -> dt.date:
        """The partition's day, for daily time partitioning only.

        The typed reading of ``context.partition.value``: the one accessor that hands an asset a
        real ``date`` without narrowing a type first, and it asserts the daily granularity rather
        than assuming it.

        Raises:
            AttributeError: If the asset is not time-partitioned at the daily granularity, no
                window was given, or the window covers several partitions.
        """
        partitioning = self._asset.partitioning
        if partitioning is None:
            raise AttributeError("`context.partition_date` is not available, asset is not partitioned.")
        if not isinstance(partitioning, TimePartitionConfig):
            raise AttributeError("`context.partition_date` is not available, asset is not time-partitioned.")
        if partitioning.granularity is not TimeGranularity.DAY:
            raise AttributeError(
                f"`context.partition_date` is not available, asset is partitioned by "
                f"{partitioning.granularity.value}. Use `context.partition` instead."
            )
        window = self._require_window("partition_date")
        if len(window) > 1:
            raise AttributeError(
                f"`context.partition_date` is not available, the run covers {len(window)} partitions."
            )
        partition = window.first
        assert isinstance(partition, TimePartition)
        return partition.value

    def _require_window(self, accessor: str) -> PartitionWindow:
        """Return the run's window, or explain why there is none.

        Args:
            accessor: Name of the calling accessor, quoted back in the error message.

        Returns:
            The window.

        Raises:
            AttributeError: If the asset is not partitioned, or no window was given.
        """
        if self._asset.partitioning is None:
            raise AttributeError(f"`context.{accessor}` is not available, asset is not partitioned.")
        if self._window is None:
            raise AttributeError(f"`context.{accessor}` is not available, no window provided.")
        return self._window
```

- [ ] **Step 4: Write `destination/context.py`**

```python
"""Frozen context object passed to every IO read/write call."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from interloper.errors import ConfigError
from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.schema import Schema


@dataclass(frozen=True)
class IOContext:
    """Immutable context passed to `Destination.read` and `Destination.write`.

    Carries the target asset, the window the call covers and the run metadata, so a destination
    resolves the storage location, plus the *effective* schema of the data: the asset's declared
    schema when set, otherwise the one inferred during conform, ``None`` when none could be
    resolved. Destinations use it for DDL, typed loads and restoring types on read.

    A destination stores data per **partition**, ``None`` standing for the unpartitioned whole;
    `partitions` and `slices` spell that out, so no destination branches on the window itself.
    """

    asset: Asset
    window: PartitionWindow | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema: type[Schema] | None = None

    @property
    def partitions(self) -> list[Partition | None]:
        """The partitions this context covers, first to last.

        Returns:
            ``[None]`` for the unpartitioned whole, else the window's partitions.
        """
        return [None] if self.window is None else list(self.window.partitions)

    def slices(self, data: Any) -> list[tuple[Partition | None, Any]]:
        """Pair each partition with its slice of *data*.

        A window of several partitions is split on the asset's partition column through the
        data's representation; a single partition, or the whole, receives the data as is.

        Args:
            data: The data being written, in its native representation.

        Returns:
            ``(partition, slice)`` pairs, one per partition, in `partitions` order.

        Raises:
            ConfigError: If a window of several partitions is written for an asset that declares
                no partitioning, since nothing says which column to slice on.
        """
        partitions = self.partitions
        if len(partitions) == 1:
            return [(partitions[0], data)]
        if self.asset.partitioning is None:
            raise ConfigError(f"Asset '{self.asset.key}' is not partitioned and cannot be written over a window")
        column = self.asset.partitioning.column
        return [(partition, partition.slice(data, column)) for partition in partitions if partition is not None]
```

- [ ] **Step 5: Rename the scope across core**

Mechanical pass over `packages/interloper-core/src` and `packages/interloper-core/tests`:

```bash
grep -rl "partition_or_window" packages/interloper-core/src packages/interloper-core/tests | xargs sed -i '' 's/partition_or_window/window/g'
grep -rl "effective_partition" packages/interloper-core/src packages/interloper-core/tests | xargs sed -i '' 's/effective_partition/effective_window/g'
```

Then the manual fixes the sed cannot make:

`operation/base.py`:
- `OperationContext.window: PartitionWindow | None = None` (drop the `Partition` import from the `TYPE_CHECKING` block).
- `effective_window`:

```python
    def effective_window(self, window: PartitionWindow | None) -> PartitionWindow | None:
        """The window this operation actually consumes.

        Unpartitioned operations ignore any window the run was given.

        Args:
            window: The window the run was scoped to, ``None`` when it was unscoped.

        Returns:
            The window unchanged for a partitioned operation, ``None`` otherwise.
        """
        return window if self.partitioning is not None else None
```

- `_event_metadata(self, metadata, window=None)`: the key it writes is still `"window"` after the sed; keep it so.

`asset/base.py`:
- The four entry points and every helper now read `window: Partition | PartitionWindow | None = None` on the public ones (`run`, `run_async`, `materialize`, `materialize_async`) and `window: PartitionWindow | None` on the private ones. Update every docstring line the sed touched so it reads "The window the run covers; a partition is accepted as the window of one".
- `materialize_async`: first line `window = self.effective_window(PartitionWindow.of(window))`, then `result = await self.run_async(window, dag, metadata)` and `await self._destination_write(window, metadata, result)`.
- `run_async`: first lines

```python
        window = self.effective_window(PartitionWindow.of(window))
        self._check_window(window)
        metadata = metadata or {}
        context = ExecutionContext(self, window, metadata)
        kwargs = await self._build_kwargs(context, dag)
```

- `_build_kwargs(self, context: ExecutionContext, dag: DAG | None)`: read the window as `context.window` only where a partitioned asset is guaranteed; use the private `context._window` nowhere. Pass `self._window_of(context)` nowhere either: simply keep a local `window` variable in `run_async` and pass it: `kwargs = await self._build_kwargs(context, window, dag)`, with the signature `_build_kwargs(self, context, window, dag)`. `_read_upstreams(name, targets, dag, window, metadata)`.
- `_destination_write(self, window, metadata, result)`: build `IOContext(asset=self, window=window, metadata=metadata, schema=...)` (no second narrowing: the window is already effective).
- `_destination_read(self, upstream, window, metadata)`: `window = upstream.effective_window(window)` then `IOContext(asset=upstream, window=window, ...)`.
- `_event_metadata(self, metadata, window=None)` keeps writing the `"window"` key.

`runner/base.py`: `run(self, dag, window: Partition | PartitionWindow | None = None, metadata=None)`; first line of the body `window = PartitionWindow.of(window)`; `_init_run(dag, window, metadata)` calls `self._preflight(dag, window)`.

`runner/state.py`: `self.window: PartitionWindow | None = None`; `start_run(self, window)`.

`runner/results.py`: `RunResult.window: PartitionWindow | None = None`; `__str__` builds `parts = [f"status={self.status.value}", f"window={self.window}", ...]` with no `isinstance` branch; drop the `Partition` import.

`destination/database.py`: `write` reads `if len(context.partitions) == 1:` where it read `if not context.window:` (Task 4 rewrites this method; this keeps it correct meanwhile), and `_warn_missing_partition_column` reads `if context.window is None or ...`.

`dag/base.py`: `materialize(self, window=None)` and `materialize_async(self, window=None)`; the span attribute line reads `if window is not None: span_attrs[attributes.PARTITION] = str(window)`.

`runner/multi_process.py`: `_worker(operation_id, dag_spec, window, metadata)` builds `OperationContext(window=operation.effective_window(window), dag=dag, metadata=metadata)`.

- [ ] **Step 6: Fix the tests the sed could not**

- `tests/destination/test_context.py` and `tests/asset/test_context.py` were replaced in Step 1.
- `tests/runner/test_results.py`: `test_names_the_partition` expects `"window=2026-06-01"`, the unscoped test expects `"window=None"`, `test_names_the_window` expects `"window=2026-06-01..2026-06-03"`.
- `tests/operation/test_base.py`: `operation.effective_window(None) is None`.
- Every `IOContext(asset=..., window=TimePartition(...))` the sed produced must become `window=TimePartition(...).window`; grep `window=TimePartition(` and `window=partition)` in `tests/destination/` and wrap with `.window` (or build `partition.window` once per test).
- `tests/asset/test_base.py`: calls such as `asset.run(partition)` keep working through the sugar; `_destination_read(..., partition, ...)` direct calls, if any, pass `partition.window`.

- [ ] **Step 7: Run the core suite, ruff and ty**

Run: `uv run pytest packages/interloper-core -q && uv run ruff check packages/interloper-core && uv run ty check packages/interloper-core`
Expected: green. Fix any test that still constructs a context with the old keywords.

---

### Task 4: One table per read

**Files:**
- Modify: `packages/interloper-core/src/interloper/representation/base.py` (`Representation.concat`, `View.concat`, `RowsRepresentation.concat`)
- Modify: `packages/interloper-core/src/interloper/destination/base.py` (`read`)
- Modify: `packages/interloper-core/src/interloper/destination/database.py` (`write`, `write_partition`, `read`, `read_partition`)
- Modify: `packages/interloper-core/tests/representation/test_base.py`, `tests/destination/test_base.py`, `tests/destination/test_database.py`

**Interfaces:**
- Consumes: `IOContext.partitions`, `PartitionWindow.filter(column)`, `Partition.window` from Tasks 1 and 3.
- Produces: `Representation.concat(tables: list[Any]) -> Any` (abstract), `View.concat(others: list[Any]) -> Any`; `Destination.read(context) -> Any` returning one table; `DatabaseDestination.read(context)` as one filtered select; `DatabaseDestination.read_partition` and `write_partition` as `read`/`write` over the partition's window.

- [ ] **Step 1: Write the failing representation tests**

Append to `TestRowsRepresentation` in `tests/representation/test_base.py`:

```python
    def test_concat_flattens_in_order(self):
        rows = RowsRepresentation().concat([[{"a": 1}], [], [{"a": 2}, {"a": 3}]])
        assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]
```

Append to `TestView`:

```python
    def test_concat_appends_the_others(self):
        view = Representation.of([{"a": 1}])
        assert view.concat([[{"a": 2}], [{"a": 3}]]) == [{"a": 1}, {"a": 2}, {"a": 3}]
```

- [ ] **Step 2: Write the failing destination tests**

In `tests/destination/test_base.py`, class `TestReadDispatch`, replace `test_window_read_returns_one_result_per_partition` with:

```python
    def test_window_read_returns_one_table_in_window_order(self):
        il.MemoryDestination.clear()
        destination = il.MemoryDestination()
        asset = partitioned_asset()
        for day in (7, 8, 9):
            partition = TimePartition(datetime.date(2026, 9, day))
            destination.write(IOContext(asset=asset, window=partition.window), [{"date": f"2026-09-0{day}"}])
        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 9))

        assert destination.read(IOContext(asset=asset, window=window)) == [
            {"date": "2026-09-07"},
            {"date": "2026-09-08"},
            {"date": "2026-09-09"},
        ]

    def test_a_window_of_non_tabular_data_is_an_error(self):
        from interloper.errors import RepresentationError

        class Opaque(il.Destination):
            def write_partition(self, context, partition, data):
                return None

            def read_partition(self, context, partition):
                return object()

            def partition_row_counts(self, context):
                return {}

        window = TimePartitionWindow(datetime.date(2026, 9, 7), datetime.date(2026, 9, 8))
        with pytest.raises(RepresentationError, match="object"):
            Opaque().read(IOContext(asset=partitioned_asset(), window=window))
```

`partitioned_asset` is the module's existing daily fixture.

In `tests/destination/test_database.py`, append to `TestWrite`:

```python
    def test_window_reads_as_one_bounded_select(self):
        destination = RecordingDatabase(id="db")
        window = TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))
        destination.read(make_io_context(partitioned_asset(), window))
        where = PartitionFilter("date", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 4)))
        assert destination.calls == [("select", ("partitioned_asset", None, where))]

    def test_a_list_window_reads_by_membership(self):
        @il.asset(partitioning=ListPartitionConfig(column="region", values=("eu", "us", "apac"), allow_window=True))
        def regional(context: il.ExecutionContext) -> list:
            return []

        destination = RecordingDatabase(id="db")
        window = ListPartitionWindow("eu", "us", ("eu", "us", "apac"))
        destination.read(make_io_context(regional(), window))
        assert destination.calls == [("select", ("regional", None, PartitionFilter("region", values=("eu", "us"))))]

    def test_read_partition_is_read_over_the_partitions_window(self):
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        destination.read_partition(make_io_context(partitioned_asset()), partition)
        where = PartitionFilter("date", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))
        assert destination.calls == [("select", ("partitioned_asset", None, where))]

    def test_write_partition_replaces_that_partition(self):
        destination = RecordingDatabase(id="db")
        partition = TimePartition(datetime.date(2024, 1, 1))
        destination.write_partition(make_io_context(partitioned_asset()), partition, [{"date": "2024-01-01"}])
        where = PartitionFilter("date", bounds=(datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))
        assert destination.calls == [
            ("delete", ("partitioned_asset", None, where)),
            ("insert", ("partitioned_asset", None, [{"date": "2024-01-01"}])),
        ]
```

Import `ListPartitionConfig, ListPartitionWindow` from `interloper.partitioning`. The `make_io_context(asset, window=None, schema=None)` helper now takes a window; update its body to `IOContext(asset=asset, window=window, schema=schema)` and every call that passes a bare `partition` to pass `partition.window`.

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/representation/test_base.py packages/interloper-core/tests/destination/test_base.py packages/interloper-core/tests/destination/test_database.py -q`
Expected: FAIL (`concat` missing; window reads return lists; `read_partition` selects per partition).

- [ ] **Step 4: Add `concat` to the representation seam**

In `representation/base.py`, add to `Representation` after `infer`:

```python
    @abstractmethod
    def concat(self, tables: list[Any]) -> Any:
        """Return several tables as one, in order.

        Args:
            tables: The tables to join, all in this representation's own type, at least one.
        """
```

Add to `View` after `to`:

```python
    def concat(self, others: list[Any]) -> Any:
        """Return the viewed data followed by *others*, as one table.

        Args:
            others: Further tables in the same representation, in the order they follow.

        Returns:
            One table in the data's own representation.
        """
        return self.representation.concat([self.data, *others])
```

Add to `RowsRepresentation`:

```python
    def concat(self, tables: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
        """Return the rows of every table, in order.

        Args:
            tables: The row lists to join.

        Returns:
            One row list.
        """
        return [row for table in tables for row in table]
```

Update the module docstring's list of views ("records, columns, partition filtering") to mention concatenation.

- [ ] **Step 5: `Destination.read` returns one table**

In `destination/base.py`, import `Representation` from `interloper.representation` and replace `read`:

```python
    def read(self, context: IOContext) -> Any:
        """Read the data the context covers, as one table.

        Every partition in ``context.partitions`` is read through `read_partition`; several are
        joined through the data's representation, so a window comes back as one table in whatever
        type the destination holds natively.

        Args:
            context: IO context carrying the target asset, the window and the effective schema.

        Returns:
            One table, whatever the window.

        Raises:
            RepresentationError: If a window's parts are of no registered representation and so
                cannot be joined.
        """
        parts = [self.read_partition(context, partition) for partition in context.partitions]
        if len(parts) == 1:
            return parts[0]
        return Representation.of(parts[0]).concat(parts[1:])
```

Update the class docstring sentence "gathering a window read into one result per partition" to "joining a window read into one table".

- [ ] **Step 6: One write path and one read path on `DatabaseDestination`**

Replace the `# -- Destination interface` section of `destination/database.py` with:

```python
    # -- Destination interface -------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Replace every partition the context covers with the data, in one batch.

        Rows carry the partition column, so a database need not store per partition: the rows of
        every partition the window covers are deleted, then the data is inserted once (one load
        job rather than one per day). The unpartitioned whole is the same path with no filter.

        Args:
            context: IO context carrying the target asset, the window and the effective schema.
            data: The data to write, in its native representation.
        """
        if is_empty(data):
            return
        self._warn_missing_partition_column(data, context)
        table, dataset = self._target(context)
        with self.transaction():
            for partition in context.partitions:
                self.delete(table, dataset, self._filter(context, partition))
            self.insert(table, dataset, data, context)

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Replace one partition's rows: `write` over the partition's own window.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the whole table.
            data: The data to store, in its native representation.
        """
        self.write(replace(context, window=partition.window if partition is not None else None), data)

    def read(self, context: IOContext) -> Any:
        """Select the rows the context covers, a window as one bounded select.

        Args:
            context: IO context carrying the target asset, the window and the effective schema.

        Returns:
            The rows, in the backend's native representation.
        """
        table, dataset = self._target(context)
        where = None if context.window is None else context.window.filter(self._partition_column(context))
        return self.select(table, dataset, where)

    def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
        """Load one partition: `read` over the partition's own window.

        Args:
            context: IO context whose asset supplies the table, dataset, and partition column.
            partition: The partition to load, or ``None`` for the whole table.

        Returns:
            The partition's rows, in the backend's native representation.
        """
        return self.read(replace(context, window=partition.window if partition is not None else None))
```

Add `from dataclasses import dataclass, replace` (drop `dataclass` if nothing else in the module uses it now that `PartitionFilter` moved). Update the class docstring: "a partition's rows are deleted before its data is inserted, a window in one batch. Reads come back ... " becomes "The base owns replacing: the rows of every partition a write covers are deleted, then the data is inserted once; a read is one select filtered to the window."

- [ ] **Step 7: Run the core suite, ruff and ty**

Run: `uv run pytest packages/interloper-core -q && uv run ruff check packages/interloper-core && uv run ty check packages/interloper-core`
Expected: green.

---

### Task 5: Observation

**Files:**
- Modify: `packages/interloper-core/src/interloper/component/base.py` (`event_metadata`)
- Create: `packages/interloper-core/src/interloper/events/step.py`
- Rewrite: `packages/interloper-core/src/interloper/events/logger.py`
- Modify: `packages/interloper-core/src/interloper/events/__init__.py` (export `Step`, `StepKind`)
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (the three steps, the LOG events, `_span_attributes`; delete `_event_metadata`)
- Modify: `packages/interloper-core/src/interloper/operation/base.py` (delete `_event_metadata`)
- Modify: `packages/interloper-core/src/interloper/asset/context.py` (`logger`)
- Modify: `packages/interloper-core/src/interloper/runner/state.py` (metadata through `event_metadata`), `runner/async_runner.py` (span attributes)
- Create: `packages/interloper-core/tests/events/test_step.py`
- Rewrite: `packages/interloper-core/tests/events/test_logger.py`
- Modify: `packages/interloper-core/tests/component/test_base.py`, `tests/telemetry/test_attributes.py`, `tests/asset/test_base.py`

**Interfaces:**
- Produces: `Component.event_metadata(metadata, window=None, **extra) -> dict[str, Any]`; `StepKind.{ASSET_DATA, DEST_READ, DEST_WRITE}` with `.started/.completed/.failed -> EventType`, `.span -> str`, `.message(phase, subject, error=None) -> str`; `Step(kind, metadata, *, subject, attributes=None)` context manager with `.record(**extra)`; `EventLogger(component, metadata, window=None)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/component/test_base.py`:

```python
class TestEventMetadata:
    """One builder for every event's metadata."""

    def test_carries_identity_run_metadata_and_window(self):
        import datetime as dt

        from interloper.partitioning import TimePartition

        component = FakeComponent(id="c1")
        built = component.event_metadata({"run_id": "r1"}, TimePartition(dt.date(2026, 1, 1)).window, attempt=2)
        assert built == {
            "run_id": "r1",
            "component_id": "c1",
            "component_kind": FakeComponent.kind,
            "component_key": FakeComponent.key,
            "qualified_key": FakeComponent.key,
            "window": "2026-01-01",
            "attempt": 2,
        }

    def test_window_is_none_when_unscoped_and_parent_id_only_with_a_parent(self):
        component = FakeComponent(id="c1")
        assert component.event_metadata({})["window"] is None
        assert "parent_id" not in component.event_metadata({})
        parent = FakeComponent(id="p1")
        component.parent = parent
        assert component.event_metadata({})["parent_id"] == "p1"
        assert component.event_metadata({})["qualified_key"] == f"{FakeComponent.key}.{FakeComponent.key}"
```

Create `tests/events/test_step.py`:

```python
"""Tests for ``interloper.events.step``."""

from __future__ import annotations

import pytest

from interloper.events import Event, EventBus, EventType
from interloper.events.step import Step, StepKind
from interloper.telemetry import attributes


def _capture(body: object) -> list[Event]:
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        body()  # ty: ignore[call-non-callable]
    finally:
        EventBus.flush(timeout=5.0)
        EventBus.unsubscribe(captured.append)
    return captured


class TestStepKind:
    def test_names_its_event_types(self):
        assert StepKind.DEST_WRITE.started is EventType.DEST_WRITE_STARTED
        assert StepKind.DEST_WRITE.completed is EventType.DEST_WRITE_COMPLETED
        assert StepKind.DEST_WRITE.failed is EventType.DEST_WRITE_FAILED
        assert StepKind.ASSET_DATA.started is EventType.ASSET_DATA_STARTED
        assert StepKind.DEST_READ.failed is EventType.DEST_READ_FAILED

    def test_names_its_span(self):
        assert StepKind.ASSET_DATA.span == "interloper.asset.data"
        assert StepKind.DEST_READ.span == "interloper.destination.read"
        assert StepKind.DEST_WRITE.span == "interloper.destination.write"

    def test_keeps_the_wording(self):
        assert StepKind.ASSET_DATA.message("started", "'orders'") == "Executing 'orders'"
        assert StepKind.ASSET_DATA.message("completed", "'orders'") == "Executed 'orders'"
        assert StepKind.ASSET_DATA.message("failed", "'orders'", "ValueError: x") == "Execution of 'orders' failed: ValueError: x"
        assert StepKind.DEST_WRITE.message("started", "'orders'") == "Writing 'orders'"
        assert StepKind.DEST_WRITE.message("completed", "'orders'") == "Wrote 'orders'"
        assert StepKind.DEST_WRITE.message("failed", "'orders'", "boom") == "Failed to write 'orders': boom"
        assert StepKind.DEST_READ.message("started", "'orders'") == "Reading 'orders'"
        assert StepKind.DEST_READ.message("completed", "'orders'") == "Read 'orders'"
        assert StepKind.DEST_READ.message("failed", "'orders'", "boom") == "Failed to read 'orders': boom"


class TestStep:
    def test_a_completing_body_emits_started_then_completed_with_the_recorded_extra(self):
        metadata = {"run_id": "r1", "component_id": "c1"}

        def body() -> None:
            with Step(StepKind.DEST_WRITE, metadata, subject="'orders'") as step:
                step.record(rows=3)

        events = _capture(body)
        assert [event.type for event in events] == [EventType.DEST_WRITE_STARTED, EventType.DEST_WRITE_COMPLETED]
        assert events[0].metadata == {**metadata, "message": "Writing 'orders'"}
        assert events[1].metadata == {**metadata, "rows": 3, "message": "Wrote 'orders'"}

    def test_a_raising_body_emits_failed_and_re_raises(self):
        metadata = {"run_id": "r1", "component_id": "c1"}

        def body() -> None:
            with pytest.raises(ValueError, match="boom"), Step(StepKind.ASSET_DATA, metadata, subject="'orders'"):
                raise ValueError("boom")

        events = _capture(body)
        assert [event.type for event in events] == [EventType.ASSET_DATA_STARTED, EventType.ASSET_DATA_FAILED]
        failed = events[1].metadata
        assert failed["error"] == "ValueError: boom"
        assert failed["message"] == "Execution of 'orders' failed: ValueError: boom"
        assert "ValueError: boom" in failed["traceback"]
        assert EventType.ASSET_DATA_COMPLETED not in [event.type for event in events]

    def test_the_span_carries_the_metadata_attributes_and_the_extra_ones(self, span_exporter):
        with Step(StepKind.DEST_READ, {"run_id": "r1"}, subject="'x'", attributes={attributes.UPSTREAM_KEY: "x"}):
            pass
        [span] = [span for span in span_exporter.get_finished_spans() if span.name == "interloper.destination.read"]
        assert span.attributes[attributes.RUN_ID] == "r1"
        assert span.attributes[attributes.UPSTREAM_KEY] == "x"
```

`span_exporter` is the suite's existing in-memory exporter fixture, the one `tests/runner/test_base.py::TestTelemetrySpans` uses.

Replace `tests/events/test_logger.py` with:

```python
"""Tests for ``interloper.events.logger``."""

from __future__ import annotations

import datetime as dt

import interloper as il
from interloper.events import Event, EventBus, EventType
from interloper.events.logger import EventLogger
from interloper.partitioning import TimePartition


@il.asset(partitioning=il.TimePartitionConfig(column="date"))
def orders(context: il.ExecutionContext) -> list:  # noqa: D103
    return []


def _capture(emit: object) -> list[Event]:
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        emit()  # ty: ignore[call-non-callable]
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)
    return [event for event in captured if event.type is EventType.LOG]


def test_log_events_carry_the_component_identity_the_run_and_the_window() -> None:
    asset = orders(id="a1")
    logger = EventLogger(asset, {"run_id": "r1"}, TimePartition(dt.date(2026, 1, 1)).window)

    [event] = _capture(lambda: logger.info("hello"))

    assert event.metadata == {
        "run_id": "r1",
        "component_id": "a1",
        "component_kind": "asset",
        "component_key": "orders",
        "qualified_key": "orders",
        "window": "2026-01-01",
        "level": "INFO",
        "message": "hello",
    }


def test_each_level_is_its_name() -> None:
    logger = EventLogger(orders(), {})
    levels = [event.metadata["level"] for event in _capture(lambda: (logger.debug("d"), logger.warning("w"), logger.error("e")))]
    assert levels == ["DEBUG", "WARNING", "ERROR"]
```

In `tests/telemetry/test_attributes.py`, confirm `test_maps_event_metadata_keys` reads `"window": "2026-07-01"` (the Task 3 rename did it) and maps to `attributes.PARTITION`.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/events/test_step.py packages/interloper-core/tests/events/test_logger.py packages/interloper-core/tests/component/test_base.py::TestEventMetadata -q`
Expected: FAIL (`interloper.events.step` missing, `event_metadata` missing, old `EventLogger` signature).

- [ ] **Step 3: `Component.event_metadata`**

In `component/base.py`, import `Mapping` (already) and, under `TYPE_CHECKING`, `from interloper.partitioning.base import PartitionWindow`. Add in the `# -- Identity` section after `__str__`:

```python
    def event_metadata(
        self,
        metadata: Mapping[str, Any],
        window: PartitionWindow | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """Build the metadata of an event about this component.

        One shape for every event: the run metadata, the component's identity, the window the
        event is scoped to (``None`` when unscoped), the parent's id when the component has one,
        and whatever the producer adds (a message, an error, a destination key, an attempt).

        Args:
            metadata: Run-level metadata (run id, backfill id, anything the caller passed).
            window: The window the event is scoped to, or ``None``.
            **extra: Producer-specific keys, written last so they win.

        Returns:
            The event metadata.
        """
        built: dict[str, Any] = {
            **metadata,
            "component_id": self.id,
            "component_kind": self.kind,
            "component_key": self.key,
            "qualified_key": self.qualified_key,
            "window": str(window) if window is not None else None,
        }
        if self._parent is not None:
            built["parent_id"] = self._parent.id
        built.update(extra)
        return built
```

- [ ] **Step 4: Write `events/step.py`**

```python
"""Step: one observed lifecycle step, a started, completed and failed event triplet around a span."""

from __future__ import annotations

import traceback
from enum import Enum
from types import TracebackType
from typing import Any

from interloper.errors import format_exception
from interloper.events.bus import EventBus
from interloper.events.types import EventType
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer


class StepKind(Enum):
    """The lifecycle steps the framework observes, each owning its events, span and wording."""

    ASSET_DATA = "asset_data"
    DEST_READ = "dest_read"
    DEST_WRITE = "dest_write"

    @property
    def started(self) -> EventType:
        """The event emitted when the step begins."""
        return EventType(f"{self.value}_started")

    @property
    def completed(self) -> EventType:
        """The event emitted when the step's body returns."""
        return EventType(f"{self.value}_completed")

    @property
    def failed(self) -> EventType:
        """The event emitted when the step's body raises."""
        return EventType(f"{self.value}_failed")

    @property
    def span(self) -> str:
        """The name of the span the step opens."""
        return _SPANS[self]

    def message(self, phase: str, subject: str, error: str | None = None) -> str:
        """Render the event message for one phase of the step.

        Args:
            phase: ``"started"``, ``"completed"`` or ``"failed"``.
            subject: What the step acts on, quoted as the message shows it.
            error: The formatted error, for the failed phase.

        Returns:
            The message.
        """
        started, completed, failed = _PHRASING[self]
        if phase == "started":
            return started.format(subject=subject)
        if phase == "completed":
            return completed.format(subject=subject)
        return failed.format(subject=subject, error=error)


_SPANS = {
    StepKind.ASSET_DATA: "interloper.asset.data",
    StepKind.DEST_READ: "interloper.destination.read",
    StepKind.DEST_WRITE: "interloper.destination.write",
}

_PHRASING = {
    StepKind.ASSET_DATA: ("Executing {subject}", "Executed {subject}", "Execution of {subject} failed: {error}"),
    StepKind.DEST_READ: ("Reading {subject}", "Read {subject}", "Failed to read {subject}: {error}"),
    StepKind.DEST_WRITE: ("Writing {subject}", "Wrote {subject}", "Failed to write {subject}: {error}"),
}


class Step:
    """One observed step: its started event and span on entry, its completed or failed event on exit.

    A context manager wrapping the body of a lifecycle step. Entering emits the kind's started
    event and opens its span with the metadata mapped to attributes; a body that returns emits
    the completed event carrying whatever `record` added; a body that raises emits the failed
    event with the formatted error and the traceback, then re-raises.

    ```py
    with Step(StepKind.DEST_WRITE, asset.event_metadata(metadata, window), subject=f"'{asset.key}'") as step:
        await invoke(destination.write, context, data)
        step.record(rows=len(data))
    ```
    """

    def __init__(
        self,
        kind: StepKind,
        metadata: dict[str, Any],
        *,
        subject: str,
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Describe the step before entering it.

        Args:
            kind: Which step this is.
            metadata: The event metadata, as `Component.event_metadata` builds it.
            subject: What the step acts on, quoted as the messages show it.
            attributes: Span attributes beyond the ones the metadata maps to.
        """
        self._kind = kind
        self._metadata = metadata
        self._subject = subject
        self._attributes = {**telemetry_attributes.from_metadata(metadata), **(attributes or {})}
        self._extra: dict[str, Any] = {}
        self._span: Any = None

    def record(self, **extra: Any) -> None:
        """Add metadata to the completed event.

        Args:
            **extra: Keys the completed event carries on top of the step's metadata.
        """
        self._extra.update(extra)

    def __enter__(self) -> Step:
        """Emit the started event and open the span.

        Returns:
            The step, for `record`.
        """
        EventBus.emit(self._kind.started, metadata={**self._metadata, "message": self._kind.message("started", self._subject)})
        self._span = tracer().start_as_current_span(self._kind.span, attributes=self._attributes)
        self._span.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """Close the span and emit the completed or failed event.

        Args:
            exc_type: The exception type, if the body raised.
            exc: The exception, if the body raised.
            tb: The traceback, if the body raised.

        Returns:
            ``False``, so an exception propagates.
        """
        self._span.__exit__(exc_type, exc, tb)
        if exc is None:
            message = self._kind.message("completed", self._subject)
            EventBus.emit(self._kind.completed, metadata={**self._metadata, **self._extra, "message": message})
            return False
        error = format_exception(exc)
        EventBus.emit(
            self._kind.failed,
            metadata={
                **self._metadata,
                "error": error,
                "traceback": traceback.format_exc(),
                "message": self._kind.message("failed", self._subject, error),
            },
        )
        return False
```

Export both from `events/__init__.py` (`from interloper.events.step import Step, StepKind`, added to `__all__`).

- [ ] **Step 5: Rewrite `events/logger.py`**

```python
"""Event-based logger emitting LOG events on the event bus."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from interloper.events.bus import EventBus
from interloper.events.types import EventType

if TYPE_CHECKING:
    from interloper.component.base import Component
    from interloper.partitioning.base import PartitionWindow


class EventLogger:
    """Logger that emits messages as ``LOG`` events attributed to one component.

    Provides the familiar ``debug`` / ``info`` / ``warning`` / ``error`` interface; each call
    emits an ``EventType.LOG`` event carrying the component's identity, the run metadata and the
    window, built through `Component.event_metadata`, plus ``level`` and ``message``.

    ```py
    context.logger.info("Fetched 142 records")
    ```
    """

    def __init__(
        self,
        component: Component,
        metadata: Mapping[str, Any],
        window: PartitionWindow | None = None,
    ) -> None:
        """Bind the logger to a component and a run.

        Args:
            component: The component the events are attributed to.
            metadata: Run metadata carried on every event.
            window: The window the run covers, carried on every event; ``None`` when unscoped.
        """
        self._component = component
        self._metadata = metadata
        self._window = window

    def _emit(self, level: int, message: str) -> None:
        """Emit a ``LOG`` event at a level.

        Args:
            level: Standard `logging` level constant, carried on the event as its level name.
            message: The log message.
        """
        metadata = self._component.event_metadata(
            self._metadata, self._window, level=logging.getLevelName(level), message=message
        )
        EventBus.emit(EventType.LOG, metadata=metadata)

    def debug(self, message: str) -> None:
        """Emit a debug-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.DEBUG, message)

    def info(self, message: str) -> None:
        """Emit an info-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.INFO, message)

    def warning(self, message: str) -> None:
        """Emit a warning-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.WARNING, message)

    def error(self, message: str) -> None:
        """Emit an error-level log event.

        Args:
            message: The log message.
        """
        self._emit(logging.ERROR, message)
```

In `asset/context.py`, `logger` becomes `self._logger = EventLogger(self._asset, self._metadata, self._window)` and the `parent` lookup goes.

- [ ] **Step 6: The asset's three steps**

In `asset/base.py`: import `Step, StepKind` from `interloper.events`; delete `_event_metadata`; delete `traceback` and `EventType`/`EventBus` imports only if nothing else uses them (the `_read_upstreams` and `_destination_write` LOG warnings still use `EventBus.emit(EventType.LOG, ...)`).

`run_async`, from the kwargs line on:

```python
        kwargs = await self._build_kwargs(context, window, dag)
        with Step(StepKind.ASSET_DATA, self.event_metadata(metadata, window), subject=f"'{self.key}'"):
            result = self._unwrap(await invoke(self.data, **kwargs))
        # Normalization + conform is CPU-bound (pandas/pyarrow); offload it so
        # it never blocks the event loop while other assets run concurrently.
        return await asyncio.to_thread(self._normalize_and_conform, result)
```

`_destination_write`:

```python
    async def _destination_write(self, window: PartitionWindow | None, metadata: dict[str, Any], result: Any) -> None:
        """Write the execution result to every configured destination.

        Args:
            window: The window the asset consumed, ``None`` for an unpartitioned asset.
            metadata: Run-level metadata (run id, backfill id), carried onto the emitted events.
            result: The normalized and conformed data to write. An empty result is skipped with a
                warning.
        """
        destinations = self._destinations()
        if not destinations:
            return
        if is_empty(result):
            EventBus.emit(
                EventType.LOG,
                metadata=self.event_metadata(
                    metadata,
                    window,
                    level="WARNING",
                    message=f"Asset '{self.key}' produced no data; skipping write to {len(destinations)} destination(s)",
                ),
            )
            return
        context = IOContext(asset=self, window=window, metadata=metadata, schema=self._effective_schema or self.schema)
        rows = len(result) if isinstance(result, Sized) else None
        for destination in destinations:
            step = Step(
                StepKind.DEST_WRITE,
                self.event_metadata(metadata, window, destination_key=destination.key),
                subject=f"'{self.key}'",
            )
            with step:
                await invoke(destination.write, context, result)
                step.record(rows=rows)
```

`_destination_read`:

```python
    async def _destination_read(self, upstream: Asset, window: PartitionWindow | None, metadata: dict[str, Any]) -> Any:
        """Read data from an upstream asset's read destination (see `_read_destination`).

        Args:
            upstream: The asset whose materialized data is read.
            window: The run's window, narrowed to what the *upstream* asset consumes.
            metadata: Run-level metadata (run id, backfill id), carried onto the emitted events.

        Returns:
            The data read from the upstream asset's destination.

        Raises:
            AssetError: If the read fails for any reason, the cause attached.
        """
        destination = upstream._read_destination()
        window = upstream.effective_window(window)
        context = IOContext(asset=upstream, window=window, metadata=metadata, schema=upstream.schema)
        step = Step(
            StepKind.DEST_READ,
            self.event_metadata(metadata, window, destination_key=destination.key),
            subject=f"'{upstream.key}'",
            attributes={telemetry_attributes.UPSTREAM_KEY: upstream.key},
        )
        try:
            with step:
                return await invoke(destination.read, context)
        except Exception as error:
            raise AssetError(f"Failed to load data from upstream asset '{upstream.key}': {format_exception(error)}") from error
```

`_read_upstreams`: both warning emissions become `EventBus.emit(EventType.LOG, metadata=self.event_metadata(metadata, window, level="WARNING", message=...))` with the same message text.

`_span_attributes`: `return telemetry_attributes.from_metadata(self.event_metadata({}))`.

In `operation/base.py`, delete `_event_metadata`.

- [ ] **Step 7: `RunState` builds metadata through the component**

In `runner/state.py`, replace `_operation_event_metadata` with:

```python
    def _operation_event_metadata(self, operation: Operation) -> dict[str, Any]:
        """Build event metadata for an operation state transition.

        Args:
            operation: The operation the event is about.

        Returns:
            The operation's event metadata, carrying the attempt the event belongs to.
        """
        return operation.event_metadata(self.metadata, self.window, attempt=self.attempts[operation.id])
```

In `start_run` and `end_run`, the run-level events keep `"window": str(self.window) if self.window else None` (the Task 3 sed produced it). In `runner/async_runner.py`, the span attributes line reads `attributes.from_metadata(operation.event_metadata(self.state.metadata, effective_window))`.

- [ ] **Step 8: Run the core suite, ruff and ty**

Run: `uv run pytest packages/interloper-core -q && uv run ruff check packages/interloper-core && uv run ty check packages/interloper-core`
Expected: green. Tests in `tests/asset/test_base.py` that asserted an event's `component_kind` or `qualified_key` on operation events now find both on every event; tests asserting exact metadata dicts gain `qualified_key` and `window`.

---

### Task 6: The CLI and the container runners

**Files:**
- Modify: `packages/interloper-core/src/interloper/cli/commands/run.py` (`_resolve_window`, `_print_plan`, `_cmd_run`)
- Modify: `packages/interloper-core/tests/cli/commands/test_run.py`
- Modify: `packages/interloper-docker/src/interloper_docker/runner.py` (`_submit_operation`, `_build_command`, delete `_handle_flushed_future`)
- Modify: `packages/interloper-k8s/src/interloper_k8s/runner.py` (`_submit_operation`, `_build_command`, delete `_handle_flushed_future` and `_cancel_all`)
- Create: `packages/interloper-docker/tests/test_runner.py`, `packages/interloper-k8s/tests/test_runner.py`

**Interfaces:**
- Consumes: `PartitionConfig.window(start_key, end_key)`, `PartitionWindow.first/last/__len__`, `Operation.effective_window`.
- Produces: `_resolve_window(args, dag) -> PartitionWindow | None` in the CLI; `_build_command(dag_spec, window: PartitionWindow | None, run_id) -> list[str]` on both container runners.

- [ ] **Step 1: Write the failing CLI tests**

In `tests/cli/commands/test_run.py`, replace `TestResolvePartition` with (`FakePartitionedSource` and `FakeRunSource` are the module's existing fixtures):

```python
class TestResolveWindow:
    """The date flags resolve through the DAG's partitioning, so a key means what the asset means."""

    def test_a_day_key_is_a_window_of_one(self) -> None:
        import datetime as dt

        from interloper.cli.commands.run import _resolve_window
        from interloper.partitioning import TimeGranularity, TimePartitionWindow

        window = _resolve_window(_args(date="2026-06-01"), il.DAG(FakePartitionedSource()))
        assert isinstance(window, TimePartitionWindow)
        assert len(window) == 1
        assert window.granularity is TimeGranularity.DAY
        assert window.start == dt.date(2026, 6, 1)

    def test_a_key_of_another_granularity_is_refused(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        with pytest.raises(SystemExit, match="is not a day key"):
            _resolve_window(_args(date="2026-06"), il.DAG(FakePartitionedSource()))

    def test_start_and_end_keys_are_a_range(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        window = _resolve_window(_args(start_date="2026-06-01", end_date="2026-06-03"), il.DAG(FakePartitionedSource()))
        assert window is not None
        assert str(window) == "2026-06-01..2026-06-03"

    def test_a_key_needs_a_partitioned_operation(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        with pytest.raises(SystemExit, match="no enabled operation of the DAG is partitioned"):
            _resolve_window(_args(date="2026-06-01"), il.DAG(FakeRunSource()))

    def test_no_flags_is_no_window(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        assert _resolve_window(_args(), il.DAG(FakeRunSource())) is None

    def test_date_and_range_cannot_be_combined(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        with pytest.raises(SystemExit, match="cannot be combined"):
            _resolve_window(_args(date="2026-06-01", start_date="2026-06-01", end_date="2026-06-02"), il.DAG(FakePartitionedSource()))
```

Update the dry-run test that asserts the plan text: the line reads `Window:` instead of `Partition:`.

- [ ] **Step 2: Write the failing container runner tests**

Create `packages/interloper-docker/tests/test_runner.py`:

```python
"""Tests for ``interloper_docker.runner``."""

from __future__ import annotations

import datetime as dt
import json

import interloper as il
from interloper.partitioning import TimeGranularity, TimePartition, TimePartitionWindow

from interloper_docker.runner import DockerRunner

SPEC = {"items": []}


def _runner() -> DockerRunner:
    return DockerRunner(image="img", reraise=False, fail_fast=False)


class TestBuildCommand:
    def test_no_window_adds_no_flags(self):
        command = _runner()._build_command(SPEC, None, "r1")
        assert command == ["interloper", "run", "--format", "inline", "--run-id=r1", json.dumps(SPEC)]

    def test_one_partition_is_a_date(self):
        command = _runner()._build_command(SPEC, TimePartition(dt.date(2026, 1, 1)).window, "r1")
        assert command[-2:] == ["--date", "2026-01-01"]

    def test_an_hourly_partition_keeps_its_hour(self):
        partition = TimePartition(dt.datetime(2026, 1, 1, 13), TimeGranularity.HOUR)
        assert _runner()._build_command(SPEC, partition.window, "r1")[-2:] == ["--date", "2026-01-01T13"]

    def test_several_partitions_are_a_range(self):
        window = TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        command = _runner()._build_command(SPEC, window, "r1")
        assert command[-4:] == ["--start-date", "2026-01-01", "--end-date", "2026-01-03"]

    def test_an_unpartitioned_operation_gets_no_window(self):
        @il.asset
        def static() -> list:
            return []

        operation = static()
        assert operation.effective_window(TimePartition(dt.date(2026, 1, 1)).window) is None
```

Create `packages/interloper-k8s/tests/test_runner.py` with the same body, importing `KubernetesRunner` from `interloper_k8s.runner` and constructing `KubernetesRunner(image="img", reraise=False, fail_fast=False)`.

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/cli/commands/test_run.py::TestResolveWindow packages/interloper-docker/tests/test_runner.py packages/interloper-k8s/tests/test_runner.py -q`
Expected: FAIL (`_resolve_window` missing; the command builders branch on `TimePartition`).

- [ ] **Step 4: `_resolve_window` in the CLI**

In `cli/commands/run.py`, replace `_resolve_partition` with:

```python
def _resolve_window(args: argparse.Namespace, dag: DAG) -> PartitionWindow | None:
    """Resolve the CLI date flags into the run's window, through the DAG's partitioning.

    The keys are parsed by the partitioning of the DAG's first enabled partitioned operation,
    so a key means what that asset means (a day key for a daily asset, a declared value for a
    list-partitioned one); the DAG already enforces that every edge shares a partitioning, and
    the runner's preflight checks the window against every operation.

    Args:
        args: Parsed CLI arguments, carrying ``date``, ``start_date`` and ``end_date``.
        dag: The DAG about to run.

    Returns:
        The window for ``--date`` (one partition) or ``--start-date``/``--end-date``, or ``None``
        when neither is set.

    Raises:
        SystemExit: If the flags are inconsistent, no operation is partitioned, or a key does
            not name a partition of the DAG's partitioning.
    """
    from interloper.errors import PartitionError

    has_date = args.date is not None
    has_window = args.start_date is not None or args.end_date is not None
    if has_date and has_window:
        raise SystemExit("Error: --date cannot be combined with --start-date/--end-date.")
    if not has_date and not has_window:
        return None
    if has_window and not (args.start_date and args.end_date):
        raise SystemExit("Error: both --start-date and --end-date must be provided.")

    partitioning = next(
        (operation.partitioning for operation in dag.operations if operation.enabled and operation.partitioning),
        None,
    )
    if partitioning is None:
        raise SystemExit("Error: a partition key was given, but no enabled operation of the DAG is partitioned.")
    start_key, end_key = (args.date, args.date) if has_date else (args.start_date, args.end_date)
    try:
        return partitioning.window(start_key, end_key)
    except PartitionError as exc:
        raise SystemExit(f"Error: {exc}") from exc
```

In `_cmd_run`: `window = _resolve_window(args, dag)` after the DAG is built; `_print_plan(dag, window, ...)`; `runner.run(dag, window, metadata=metadata or None)`. In `_print_plan`, rename the parameter to `window` and the line to `f"Window:    {window if window is not None else '(none)'}"`. The `TYPE_CHECKING` import becomes `from interloper.partitioning.base import PartitionWindow`.

- [ ] **Step 5: The container runners derive the flags from the window**

In both `interloper_docker/runner.py` and `interloper_k8s/runner.py`:
- Imports: replace the two `interloper.partitioning` lines with `from interloper.partitioning import PartitionWindow`.
- `_submit_operation(self, operation: Operation, window: PartitionWindow | None)`: the command line becomes `cmd = self._build_command(dag_spec, operation.effective_window(window), self.state.run_id)`.
- `_build_command(self, dag_spec, window: PartitionWindow | None, run_id)`, replacing the two `isinstance` branches with:

```python
        if window is not None:
            if len(window) == 1:
                cmd.extend(["--date", window.first.id])
            else:
                cmd.extend(["--start-date", window.first.id, "--end-date", window.last.id])
```

- Update the docstrings: "The window to materialize, narrowed to the operation's own; ``None`` for an unpartitioned operation."
- Delete `_handle_flushed_future` in both and `_cancel_all` in the Kubernetes runner; nothing calls them.

- [ ] **Step 6: Run the three suites, ruff and ty**

Run: `uv run pytest packages/interloper-core packages/interloper-docker packages/interloper-k8s -q && uv run ruff check packages/interloper-core packages/interloper-docker packages/interloper-k8s && uv run ty check packages/interloper-core packages/interloper-docker packages/interloper-k8s`
Expected: green.

---

### Task 7: Cross-package adoption

**Files:**
- Modify: `packages/interloper-pandas/src/interloper_pandas/representation.py`, `packages/interloper-pandas/tests/test_representation.py`
- Modify: `packages/interloper-db/src/interloper_db/store/components/base.py` (`asset_partitionings`), `packages/interloper-db/src/interloper_db/store/backfills.py` (two `partition_count()` calls)
- Modify: `packages/interloper-google-cloud/src/interloper_google_cloud/google_sheets/destination.py` (`read` instead of `read_partition`, `_matches`), `.../bigquery/destination.py` (`_predicate`, `_partition_params`)
- Modify: the partition filter renderers in `interloper-sql`, `interloper-duckdb`, `interloper-clickhouse`, `interloper-snowflake`, `interloper-databricks`, `interloper-azure`
- Modify: the tests of every destination package and of interloper-assets

**Interfaces:**
- Consumes: `PartitionFilter.values`, `Representation.concat`, `PartitionConfig.from_definition`, `len(window)`, `IOContext(window=)`, `ExecutionContext(asset, window)`.

- [ ] **Step 1: The pandas representation joins frames**

Append to `TestDataFrameRepresentation` in `packages/interloper-pandas/tests/test_representation.py`:

```python
    def test_concat_joins_frames_with_a_fresh_index(self):
        frames = [pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [2, 3]})]
        joined = DataFrameRepresentation().concat(frames)
        assert joined["a"].tolist() == [1, 2, 3]
        assert joined.index.tolist() == [0, 1, 2]
```

Run it, expect FAIL, then add to `DataFrameRepresentation`:

```python
    def concat(self, tables: list[pd.DataFrame]) -> pd.DataFrame:
        """Return the rows of every frame, in order, with a fresh index.

        Args:
            tables: The frames to join.

        Returns:
            One frame.
        """
        return pd.concat(tables, ignore_index=True)
```

- [ ] **Step 2: The db store builds partitionings from definitions and counts windows with `len`**

In `packages/interloper-db/src/interloper_db/store/components/base.py`, `asset_partitionings`, replace the hand-built `il.TimePartitionConfig(...)` with:

```python
            config = il.PartitionConfig.from_definition(partitioning)
            if isinstance(config, il.TimePartitionConfig):
                partitionings[asset_id] = config
```

and update the docstring: "Each time-partitioned asset's partitioning by row id; an asset partitioned another way has no coverage yet and is skipped." Drop the `TimeGranularity` import if the method was its only user.

In `packages/interloper-db/src/interloper_db/store/backfills.py`, both `span = window.partition_count()` become `span = len(window)`.

Run: `uv run pytest packages/interloper-db -q`. Expected: green (the existing `TestAssetPartitionings` compares configs by equality, which `from_definition` reproduces).

- [ ] **Step 3: Google Sheets post-processes `read`, and every backend renders membership**

`google_sheets/destination.py`: rename the `read_partition` override to `read(self, context: IOContext) -> list[dict[str, Any]]` with `rows = super().read(context)` as its first line and the same post-processing; update its docstring ("Load the rows the context covers, restoring the schema's types ..."). In `_matches`, replace the `bounds is None` branch with:

```python
    if where.values is not None:
        return cell in {_text(_cell(value)) for value in where.values}
    assert where.bounds is not None
    start, end = where.bounds
```

`bigquery/destination.py`, `_predicate`:

```python
    column = where.column
    if where.values is not None:
        return f"`{column}` IN UNNEST(@partition_values)", [_partition_params(bq_table, column, where.values)]
    assert where.bounds is not None
    start, end = where.bounds
```

with the return annotation widened to `tuple[str, list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]]`, and the new helper next to `_partition_param`:

```python
def _partition_params(
    bq_table: bigquery.Table,
    column: str,
    values: tuple[Any, ...],
) -> bigquery.ArrayQueryParameter:
    """Build the array parameter a membership predicate names, typed from the table.

    Each value is coerced exactly as `_partition_param` coerces a scalar, so the array's type
    and elements match the column.

    Args:
        bq_table: The target table (for column type lookup).
        column: Partition column name.
        values: The partition values.

    Returns:
        An array parameter named ``partition_values``.
    """
    scalars = [_partition_param(bq_table, column, value) for value in values]
    return bigquery.ArrayQueryParameter("partition_values", scalars[0].type_, [scalar.value for scalar in scalars])
```

`interloper_sql/destination.py`, `_predicate`: the equality line becomes `return column.in_([_coerce(column, value) for value in where.values])` guarded by `if where.values is not None:` first, bounds after an `assert where.bounds is not None`.

`interloper_duckdb/destination.py`: in the predicate method, replace the equality branch with:

```python
        if where.values is not None:
            placeholders = ", ".join(placeholder for _ in where.values)
            return f"{column} IN ({placeholders})", list(where.values)
        assert where.bounds is not None
```

`interloper_clickhouse/destination.py`:

```python
    if where.values is not None:
        return f"{column} IN {{values:Array({type_name})}}", {"values": list(where.values)}
    assert where.bounds is not None
```

`interloper_snowflake/destination.py`:

```python
    if where.values is not None:
        return f"{column} IN ({', '.join('%s' for _ in where.values)})", tuple(where.values)
    assert where.bounds is not None
```

`interloper_azure/fabric/destination.py`:

```python
    if where.values is not None:
        return f"{column} IN ({', '.join('?' for _ in where.values)})", [_bind(value) for value in where.values]
    assert where.bounds is not None
```

`interloper_databricks/destination.py`, `_predicate`: `values = [value for f in filters if f.values is not None for value in f.values]` (the rest of the function already renders one value as equality and several as a list).

- [ ] **Step 4: Update the destination packages' tests**

In every `tests/test_destination.py` (sql, duckdb, clickhouse, snowflake, databricks, aws s3, azure fabric) and the two under `interloper-google-cloud/tests`:
- `PartitionConfig(column="region")` becomes `ListPartitionConfig(column="region", values=(...))`, the values covering every region the module writes (read the module: `eu`, `us`, `eu-west` where present); `Partition("eu")` becomes `ListPartition("eu")`; import both from `interloper.partitioning`.
- Every `IOContext(..., partition_or_window=scope, ...)` and every `_ctx` helper becomes `IOContext(asset=asset, window=PartitionWindow.of(scope), schema=schema)`, importing `PartitionWindow` from `interloper.partitioning`, so the helpers keep accepting a bare partition.
- `PartitionFilter(..., value=x)` becomes `PartitionFilter(..., values=(x,))`.
- Equality assertions move to membership: fabric `[region] = ?` becomes `[region] IN (?)`; snowflake `"region" = %s` becomes `"region" IN (%s)`; clickhouse `` `region` = {value:String} `` with `{"value": "eu"}` becomes `` `region` IN {values:Array(String)} `` with `{"values": ["eu"]}`; bigquery `"= @partition_value" in sql` becomes `"IN UNNEST(@partition_values)" in sql`; databricks `test_one_value_is_equality` and `test_several_values_are_a_list` build their filters with `values=`.
- ClickHouse `test_window_reads_each_partition` becomes `test_window_reads_as_one_frame`: `frame = destination.read(_ctx(daily(), window))` and `assert frame["cost"].tolist() == [1.0, 2.0]`.
- Any test reading a window and expecting a list now expects one table in window order.

Run: `uv run pytest packages/interloper-sql packages/interloper-duckdb packages/interloper-clickhouse packages/interloper-snowflake packages/interloper-databricks packages/interloper-aws packages/interloper-azure packages/interloper-google-cloud packages/interloper-pandas -q`
Expected: green.

- [ ] **Step 5: Update interloper-assets' tests**

In the ten `test_source.py` modules that build an `ExecutionContext` (linkedin_ads, google_ads, pinterest_ads, linkedin_organic, teads, usercentrics, search_ads_360, facebook_insights, instagram_insights, amazon_selling_partner), the construction becomes `ExecutionContext(asset, il.TimePartition(DAY).window)` (or the module's own date constant), dropping `asset_key=` and `partitioning=`. In the star-schema, campaign matcher and campaign performance analysis modules, `il.IOContext(asset=..., partition_or_window=partition)` becomes `il.IOContext(asset=..., window=partition.window)`.

Run: `uv run pytest packages/interloper-assets -q`
Expected: green.

- [ ] **Step 6: The whole workspace**

Run: `uv run pytest -q && uv run ruff check && uv run ty check`
Expected: green across every package, including `interloper-scheduler` and `interloper-api`, which only consume renamed names through the runner's sugar and the definition dict.

---

### Task 8: Docs, examples and the closing pass

**Files:**
- Modify: `docs/guide/partitioning.md`, `docs/guide/assets.md`, `docs/guide/destinations.md`, `docs/guide/execution.md`, `docs/guide/events.md`, `docs/guide/dependencies.md`
- Modify: `examples/assets/demo.py`, `examples/assets/adup.py`
- Verify: `packages/interloper-core/src/interloper/__init__.py` exports

- [ ] **Step 1: `docs/guide/partitioning.md`**

- Opening paragraph: "A run of a partitioned asset always covers a **window**: one partition, or several when the asset allows it. A partition is the storage unit; the window is the scope. Partitioning is by time, or by a declared list of values."
- Delete the sentence on `WEEK` and `QUARTER`.
- "Partitions and windows" block becomes:

```py
partition = il.TimePartition(dt.date(2026, 5, 20), il.TimeGranularity.MONTH)
partition.value          # 2026-05-01, normalized to the period start
partition.id             # "2026-05"
partition.bounds         # (2026-05-01, 2026-06-01)
partition.window         # the one-partition window
partition.filter("date") # the bounds, as a database selects them

window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 7))
str(window)              # "2026-01-01..2026-01-07"; "2026-01-01" for a window of one
len(window)              # 7
list(window)             # TimePartition per day, oldest first
window.bounds            # (2026-01-01, 2026-01-08)
```

and the paragraph after it: "Both bounds are inclusive; a window ending before it starts raises `ValueError`. Every entry point (`asset.run`, `asset.materialize`, `dag.materialize`, `runner.run`) accepts a bare partition as the window of one."
- Add before "Windowed assets":

```markdown
## List partitioning

An asset partitioned by a declared list of values, such as regions or markets:

```py
@il.asset(partitioning=il.ListPartitionConfig(column="region", values=["eu", "us", "apac"]))
def sales(self, context: il.ExecutionContext) -> list[dict]:
    region = context.partition.value
    return [{"region": region, **row} for row in fetch_sales(region)]
```

The values are the partition ids, in the declared order. A window is a contiguous range of that
order: `config.window("eu", "us")` covers `eu` and `us`, and a backfill from the first value to
the last covers everything. Databases select a list partition by membership, the file and object
destinations key storage by its id. Platform support for list backfills and the run form is a
follow-up; today a list-partitioned asset runs through `asset.run`, `dag.materialize` and the
CLI.

Every partitioning answers the same questions: `check(window)` validates a run's window,
`partition(key)` parses a persisted key, `window(start_key, end_key)` builds a window,
`compatible(other)` says whether both ends of a DAG edge accept the same windows, and
`to_definition()` renders the catalog entry.
```

- "What the context exposes" table: `context.partition` "not partitioned, no window given, or the window covers several partitions"; `context.window` "The `PartitionWindow`: `.start`, `.end`, `len()`, iteration" raising "not partitioned, or no window given"; `context.partition_date` raising "not time-partitioned at `DAY`, or the window covers several partitions".
- "Validation" paragraph: "Before an asset executes, its partitioning checks the run's window: a partitioned asset with no window, a window of several partitions without `allow_window`, a window of another kind or granularity, or one reaching before `start` raise `PartitionError`, named after the asset. An unpartitioned asset ignores any window. Runners run the same check for every enabled operation before starting, listing every failing one, so a run no asset can serve fails without executing anything."
- Replace "Custom partition schemes" with: "## Extending partitioning. A new kind subclasses `il.PartitionConfig`, `il.Partition` and `il.PartitionWindow` and implements their abstract contract; the config's `kind` registers it for `from_definition`. The list kind in `interloper.partitioning.list` is the reference implementation."

- [ ] **Step 2: The other guides**

- `docs/guide/assets.md`: the running table reads `asset.run(window, dag, metadata)` and `asset.materialize(window, dag, metadata)`; the paragraph below: "`window` is required for partitioned assets and ignored for unpartitioned ones; a bare partition is accepted as the window of one." Step 1 of "What happens on materialize": "The run's window is checked by the asset's partitioning." The execution context table: replace the `context.asset_key` row with `context.asset` "The asset executing: `key`, `table`, `dataset`, `source`, `partitioning`."; `context.window` "The `PartitionWindow` the run covers; a window of one for assets without `allow_window`."; `context.partition` "The window's only partition."
- `docs/guide/destinations.md`: the IOContext row `partition_or_window` becomes `window` "The window of this call, or `None` for an unpartitioned asset; `context.partitions` lists its partitions, `None` standing for the whole."; the paragraph on `write()` and `read()`: "a window read is joined into one table through the data's representation, oldest partition first." and for `DatabaseDestination`: "rows carry the partition column, so a window clears every partition it covers and inserts the whole batch once, and reads as one select filtered to the window."
- `docs/guide/execution.md`: `result.window`; `dag.materialize(window)` wording in "Materializing"; the preflight paragraph: "the runner asks every enabled operation's partitioning to check the window and lists every failure in one `PartitionError`."
- `docs/guide/events.md`: the metadata sentence becomes "Metadata always carries what the producer knows: `run_id`, `component_id`, `component_kind`, `component_key`, `qualified_key`, `window` and, when the component has an owner, `parent_id`, plus `message` and, on failures, `error` and `traceback`. Destination events add `destination_key`, writes add `rows`, log events add `level`, operation events add `attempt`."
- `docs/guide/dependencies.md`: in "Reading upstream data", "scoped to the partition the run consumes" becomes "scoped to the window the upstream consumes, joined into one table when it covers several partitions".
- `docs/guide/telemetry.md`: the attribute table row maps `interloper.partition` to the metadata key `window`.
- `docs/guide/backfilling.md`: the event handler example reads `event.metadata["window"]`.
- `docs/extending/representations.md`: add `def concat(self, tables) -> table: ...  # several tables as one, in order` to the contract listing, with one sentence: "`concat` is how a windowed read comes back as one table."
- `docs/extending/runners.md`: `_run(dag, window, metadata) -> RunResult`, and the shared helpers list names `_preflight(dag, window)`.
- `docs/extending/operations.md`: `OperationContext` carries `window`; the method table row reads `effective_window(window)`.

- [ ] **Step 3: Examples**

`examples/assets/demo.py`: delete the `XXX` asset, the `x` instance and the `demo.b.dependencies` line; build `dag = il.DAG(demo)`; replace the `asyncio.run(...)` line with `result = dag.materialize(partition)` and print it; drop the `asyncio` import. `examples/assets/adup.py`: `results = dag.materialize(partition)`; drop the `asyncio` import; replace the two `Adup.asset_def("account")` lines with `Adup.definition().assets[0]` so the example no longer depends on `asset_def`.

- [ ] **Step 4: Exports and the generated reference**

Confirm `interloper/__init__.py` exports `ListPartition`, `ListPartitionConfig`, `ListPartitionWindow` and `interloper.events` exports `Step`, `StepKind`; `python -c "import interloper as il; il.ListPartitionConfig; from interloper.events import Step"`.

- [ ] **Step 5: The follow-up ticket**

File one ticket in ITLPR (Jira, `createJiraIssue`; if the executing session has no Jira tool, write the text into the task log for the orchestrator to file): "Platform support for list partitioning". Body: backfill expansion through `PartitionConfig.window(start_key, end_key)` instead of `TimeGranularity.period_range` in `interloper_db.store.backfills`; the run and backfill forms offering a list-partitioned asset's declared keys; the coverage insight and `asset_partitionings` covering list configs; the app's `partitionGranularity` readers, which treat a partitioning without a granularity as daily. Link the spec path.

- [ ] **Step 6: The closing checks**

Run: `make check-python`
Expected: green. Then review the whole diff once for duplication the tasks left behind (a helper written twice, a docstring that still says "partition or window", an unused import) and fold it; `git diff --stat` for the record. Do not commit.
