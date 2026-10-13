# Core phase 1: scope, contexts and observation

Date: 2026-10-13. Status: approved in conversation, implementation pending.

Phase 1 of the interloper-core consolidation programme (five stacked phases: scope, contexts and
observation; Asset and Source; the runner family; Component, Serializable and decorators; the
surface sweep). This phase settles the execution spine that the later phases are expressed in:
what a run is scoped to, what each layer hands the next, and how a lifecycle step is observed.

Scope: `interloper.partitioning`, `interloper.asset.context`, `interloper.destination.context`,
`interloper.destination.base` and `database`, `interloper.operation`, `interloper.runner` (base,
state, results, the three in-process runners), `interloper.events` (a new `step` module, the
logger), `interloper.component.base` (one method), `interloper.telemetry.attributes`, the `run`
CLI command, and the consumers those seams reach: the Docker and Kubernetes runners, the scheduler
executor, the db store's one partitioning constructor, the pandas representation, the partition
filter renderer of every database backend (BigQuery, Google Sheets, SQL, DuckDB, ClickHouse,
Snowflake, Databricks, Fabric), the destination packages' tests, interloper-assets' tests, the
docs. Splitting `Asset`, the runner walk and attempt
loops and the operation lifecycle events are phases 2 and 3.

---

## 1. Problem

The run scope is spelled `Partition | PartitionWindow | None` on nearly every signature along the
execution path, and each consumer re-derives the same three cases:

- `IOContext.partitions`, `.window` and `.slices` branch on it for destinations.
- `ExecutionContext.partition`, `.window` and `.partition_date` branch on it for authors, with two
  private helpers and an `allow_window` gate.
- `Operation.effective_partition` and the no-op `Operation._validate_time_partitioning` hook, which
  `Asset` overrides and the runner's preflight calls with the operation's own partitioning passed
  back in.
- `Asset._validate_partitioning` and `_validate_time_partitioning`, and `Runner._preflight_validation`,
  which repeats the same checks across the DAG.
- `RunResult.__str__` and the event metadata stringify it in seven places.

Three contexts carry overlapping facts. `OperationContext` holds the scope, the DAG and the run
metadata; `ExecutionContext` holds the asset key, the asset id, the source id, the partitioning,
the scope and the metadata; `IOContext` holds the asset, the scope, the metadata and the schema.
Event metadata (component id, kind, key, qualified key, parent id, scope, run id, attempt,
message, error, traceback, level, destination key, rows) is assembled by hand in five places with
slightly different keys.

The started, completed and failed event triplet around a traced span is hand-rolled four times:
the data call, the destination write, the destination read, and the operation lifecycle.

A windowed destination read returns a list of per-partition results, which `Upstream.records`
cannot interpret; `DatabaseDestination` writes a window in one batch but reads it per partition.

The partitioning layer carries a generic `Partition`, `PartitionWindow` and `PartitionConfig` under
the time layer, but no second kind exists, so the generic layer is abstract-only, the database
filter branches on `isinstance(partition, TimePartition)`, and `TimeGranularity` carries two members
(`WEEK`, `QUARTER`) that cannot be declared.

---

## 2. Decisions

1. **A run covers a window or nothing.** The scope is `PartitionWindow | None`. A partition is the
   storage unit, and a window of one at the entry points: `Asset.run`, `Asset.materialize`, their
   async twins, `DAG.materialize`, `DAG.materialize_async` and `Runner.run` accept
   `Partition | PartitionWindow | None` and normalise once through `PartitionWindow.of`.
2. **Every partitioning is an ordered space; a window is a contiguous range of it, named by its
   first and last partition.** Time orders periods and is unbounded; a list orders its declared
   values and is finite. This is what a backfill already persists (a start key and an end key), so
   the backfill model extends to lists without change.
3. **The partitioning config is the authority on its space**: it validates a window, parses a
   key, builds a window from two keys, decides compatibility with another config, and renders its
   catalog definition. `ListPartitionConfig` is delivered as the second kind, minimal and fully
   tested in core; its platform half is a follow-up.
4. **Filters are rendered by the partition or the window**, never by a destination branching on
   the kind: a time partition or window renders half-open bounds, a list partition or window
   renders membership.
5. **`Destination.read` returns one table for any scope.** Per-partition reads are concatenated
   through the data's representation; a database reads a window as one filtered select.
6. **Three contexts, one shape.** Each starts with the same facts in the same order: `asset`,
   `window`, `metadata`. `ExecutionContext` hands authors the asset instead of its key and ids.
7. **One event-metadata builder** on `Component`, one `Step` primitive for the started, completed
   and failed triplet around a span, one `EventLogger` taking the component. Event wording stays
   exactly as today; the metadata key `partition_or_window` becomes `window`.
8. **`PartitionError` is a `ConfigError`**, so a key that names no partition is a configuration
   error wherever it is raised, and the API's existing mapping answers 400.
9. Removed: `TimeGranularity.WEEK` and `QUARTER` with `SUPPORTED_GRANULARITIES`; `PartitionWindow.id`;
   `TimePartitionWindow.iter_partitions`, `partition_count` and the `__repr__` overrides;
   `Partition.slice` by equality on the base; `Operation._validate_time_partitioning`;
   `Operation._event_metadata` and the `Asset` override; `Asset._validate_partitioning` and
   `_validate_time_partitioning`; the warning for a scope handed to an unpartitioned asset;
   `ExecutionContext.asset_key` and its `asset_id`, `source_id` and `partitioning` arguments;
   `IOContext.window`; the `allow_window` gate on `context.window`.

---

## 3. Partitioning

### 3.1 Types

```py
class PartitionConfig:                          # frozen dataclass; the authority on a partition space
    column: str
    allow_window: bool = False
    def check(self, window: PartitionWindow | None) -> None
    def partition(self, key: str) -> Partition
    def window(self, start_key: str, end_key: str) -> PartitionWindow
    def compatible(self, other: PartitionConfig) -> bool
    def to_definition(self) -> dict[str, Any]
    @classmethod
    def from_definition(cls, definition: Mapping[str, Any]) -> PartitionConfig

class TimePartitionConfig(PartitionConfig):     # granularity: TimeGranularity = DAY, start: date | None = None
class ListPartitionConfig(PartitionConfig):     # values: tuple[str, ...]; partitions -> list[ListPartition]

class Partition:                                # frozen dataclass; the storage unit
    id -> str
    window -> PartitionWindow                   # the one-partition window
    def filter(self, column: str) -> PartitionFilter
    def slice(self, data: Any, column: str) -> Any
    __str__ -> id

class TimePartition(Partition):                 # value: date, granularity: TimeGranularity = DAY
    bounds -> tuple[date, date]                 # (start, end exclusive)
    @classmethod from_key(key) -> TimePartition  # granularity inferred from the key's shape

class ListPartition(Partition):                 # value: str

class PartitionWindow:                          # frozen dataclass; the run scope, a contiguous range
    start, end                                  # the first and last partition's values
    partitions -> list[Partition]               # chronological, never empty
    __iter__, __len__
    __str__ -> the partition id for one partition, "start_id..end_id" otherwise
    def filter(self, column: str) -> PartitionFilter
    @classmethod of(scope: Partition | PartitionWindow | None) -> PartitionWindow | None

class TimePartitionWindow(PartitionWindow):     # granularity; bounds; lookback() unchanged
class ListPartitionWindow(PartitionWindow):     # values: tuple[str, ...], the declared order

class PartitionFilter:                          # frozen dataclass, moved here from destination.database
    column: str
    values: tuple[Any, ...] | None = None       # rendered as membership
    bounds: tuple[Any, Any] | None = None       # rendered as >= start AND < end
```

`PartitionConfig`, `Partition` and `PartitionWindow` are abstract. Every concrete kind implements
the whole contract; nothing on the base knows about time or lists. The configs are keyword-only
dataclasses (`kw_only=True`), so a kind may add a required field after the base's defaulted
`allow_window`; every declaration already writes `column=`. `Partition` declares no fields;
`PartitionWindow` declares `start` and `end`.

### 3.2 Semantics

- `check(window)`: raises `PartitionError` when `window` is `None` ("needs a window"), when it
  holds several partitions and `allow_window` is false, or when the kind-specific check fails. The
  base implements the first two and calls a kind-specific hook for the rest. Time: the window must
  be a `TimePartitionWindow`, share the config's granularity, and not start before `start`. List:
  the window must be a `ListPartitionWindow` and every partition it holds must be one of the
  config's values. Membership, not identity: a one-partition window built from a bare
  `ListPartition` carries only its own value.
- `partition(key)`: time parses the key at the config's granularity; list requires the key to be
  one of `values`. Both raise `PartitionError` otherwise. `TimePartition.from_key` keeps inferring
  the granularity from the key's shape and raises `PartitionError` (today `ConfigError`, which it
  remains by subclassing).
- `window(start_key, end_key)`: parses both keys through `partition(key)` and requires `start`
  to come first in the space's order, else `PartitionError`.
- `compatible(other)`: time, same granularity; list, same values. Different kinds are never
  compatible.
- `to_definition()`: the catalog dict, today's `asdict` plus a `"kind"` key (`"time"` or
  `"list"`), the class's `kind` ClassVar. `from_definition` dispatches on `"kind"` through the
  subclasses registered at class creation and raises `ConfigError` when it is missing or unknown;
  each kind's `__post_init__` coerces what a JSON round trip loosens (a granularity name, an ISO
  start, a values list). `Asset.definition` uses `to_definition`.
- `ListPartitionConfig.__post_init__`: `values` is normalised to a tuple, must be non-empty, free
  of duplicates and of empty strings, else `ValueError` (as the other dataclass constructors).
- `ListPartitionConfig.partitions`: every partition in declared order, for callers that want the
  whole space.
- `Partition.filter(column)`: time renders `bounds`; list renders `values=(value,)`.
- `Partition.slice(data, column)`: time by `filter_range` over its bounds; list by `filter_eq`.
- `Partition.window`: `TimePartitionWindow(value, value, granularity)`; `ListPartitionWindow(value, value, (value,))`.
- `PartitionWindow.partitions`: time through `granularity.period_range(start, end)`, oldest
  first; list through the slice of `values` from `start` to `end` inclusive.
  `ListPartitionWindow.__post_init__` requires both bounds to be in `values` with `start` no later
  than `end`, else `ValueError`, as `TimePartitionWindow` does for an inverted range.
- `PartitionWindow.filter(column)`: time renders the window's `bounds`, which are the first
  partition's start and the last partition's exclusive end; list renders every id as `values`.
- `PartitionWindow.__str__`: `"2026-01-15"` or `"eu"` for one partition; `"2026-01-01..2026-01-15"`
  or `"eu..apac"` otherwise. Ids come from the partitions, so hour windows read
  `"2026-01-01T13..2026-01-01T15"`.
- `PartitionWindow.of(scope)`: `scope.window` for a partition, `scope` otherwise.
- `TimeGranularity` keeps `HOUR`, `DAY`, `MONTH`, `YEAR`. `key_format` is always a string;
  `format` and `parse` lose their unsupported branches; `TimePartitionConfig.__post_init__` only
  truncates `start`.
- `Partition.__str__` is the id. `TimePartition` no longer renders the ISO value.

### 3.3 Consumers

- `DatabaseDestination` builds every filter through `partition.filter(column)` or
  `window.filter(column)`. `PartitionFilter` is imported from `interloper.partitioning`;
  `interloper.destination` re-exports it so the destination packages' imports keep working.
- Every database backend renders `values` as membership (`column IN (...)`, one placeholder per
  value) where it rendered `value` by equality: BigQuery (an array parameter with `UNNEST`),
  Google Sheets (a Python set test), SQL (`column.in_`), DuckDB, ClickHouse, Snowflake, Fabric,
  and Databricks, whose predicate already lists several values.
- `DAG._check_partition_dependencies`: an unpartitioned dependent of a partitioned upstream is
  still a `DAGError`; "same granularity" becomes `upstream.partitioning.compatible(dependent.partitioning)`.
- `BigQueryDestination._time_partitioning` already creates an unpartitioned table for a non-time
  config; it is untouched.
- The db store's `asset_partitionings` builds its config through `PartitionConfig.from_definition`
  instead of hand-building a `TimePartitionConfig`, and keeps only the time configs, since its one
  consumer, the coverage insight, reads the granularity and the start; a list-partitioned asset has
  no coverage until the platform follow-up. The two readers of `partitioning["granularity"]` in the
  same store stay as they are: a list definition has no granularity and is skipped, which is
  correct for the cron window logic they feed.
- The `run` CLI resolves `--date`, `--start-date` and `--end-date` through the partitioning of the
  DAG's partitioned operations: the first one's config parses the keys (the DAG already enforces
  compatibility across operations). A key given for a DAG with no partitioned operation is an
  error. `_resolve_partition` becomes `_resolve_window` and returns `PartitionWindow | None`; the
  plan prints `Window:`.
- The Docker and Kubernetes runners pass each operation its effective window:
  `_build_command(dag_spec, operation.effective_window(window), run_id)`. One partition renders
  `--date <id>`, several render `--start-date <id> --end-date <id>`, `None` renders nothing. Ids
  come from the partitions, which also fixes the current `%Y-%m-%d` formatting that drops the hour.

---

## 4. Contexts

```py
@dataclass
class OperationContext:                 # runner -> Operation.execute
    window: PartitionWindow | None = None
    dag: DAG | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

class ExecutionContext:                 # asset -> data()
    def __init__(self, asset: Asset, window: PartitionWindow | None = None, metadata: dict | None = None)
    asset, window, metadata             # window raises AttributeError when the asset is unpartitioned or no window was given
    partition -> Partition              # the window's only partition; AttributeError when none or several
    partition_date -> date              # DAY granularity only, as today
    logger -> EventLogger               # built lazily from the asset, the window and the metadata

@dataclass(frozen=True)
class IOContext:                        # asset -> destination
    asset: Asset
    window: PartitionWindow | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema: type[Schema] | None = None
    partitions -> list[Partition | None]   # [None] for the unpartitioned whole, else window.partitions
    def slices(self, data) -> list[tuple[Partition | None, Any]]
```

- `ExecutionContext.window` and `partition` keep today's error messages, minus the `allow_window`
  gate: the config's `check` already bounds a window to one partition for assets that forbid
  windows, so `context.window` is readable on any partitioned asset.
- `partition_date` raises for a non-time partitioning ("asset is not time-partitioned").
- `IOContext.slices(data)`: the data as is when `partitions` has one entry; otherwise one slice
  per partition through `partition.slice(data, column)`. A window on an unpartitioned asset is a
  `ConfigError`, as today.
- `Operation.effective_window(window)`: the window for a partitioned operation, `None` otherwise.

### 4.1 Destination reads and writes

- `Destination.read(context)`: reads every partition in `context.partitions` through
  `read_partition` and returns one result: the single part as is, several concatenated through
  `Representation.of(parts[0]).concat(parts[1:])`. A windowed read of data no representation
  matches is a `RepresentationError`.
- `Representation.concat(self, tables)` is a new abstract method returning every table, in order,
  as one table of the representation; rows flatten the lists, the pandas representation
  concatenates frames with a fresh index. `View.concat(self, others)` returns the viewed data
  followed by `others`.
- `DatabaseDestination.write(context, data)`: one path for every scope. Inside `transaction`,
  delete the rows of every partition in `context.partitions` through its filter, then insert the
  data once. `write_partition(context, partition, data)` is that same path over the partition's
  one-partition window.
- `DatabaseDestination.read(context)`: one `select` with `context.window.filter(column)`, or no
  filter for the unpartitioned whole. `read_partition(context, partition)` is `read` over the
  partition's one-partition window. `GoogleSheetsDestination` moves its post-processing from
  `read_partition` to `read`, so a window and a single partition share it.
- `ObjectStoreDestination`, `FileDestination`, `CSVDestination` and `MemoryDestination` keep their
  per-partition hooks; they key storage by `partition.id`, which a list partition provides.

### 4.2 Asset

- `execute(context)` calls `materialize_async(context.window, context.dag, context.metadata)`.
- `run`, `run_async`, `materialize`, `materialize_async` take `window=None, dag=None, metadata=None`.
- `run_async`: `window = self.effective_window(PartitionWindow.of(window))`; a partitioned asset
  runs `self.partitioning.check(window)`; the `ExecutionContext` is built from `self`, the
  window and the metadata; the rest is unchanged in structure.
- `_build_kwargs(context, dag)` reads the window from the context; `_read_upstreams` and
  `_destination_read` take the window, the latter narrowing it through the upstream's
  `effective_window`; `_destination_write(window, metadata, result)`.
- `_span_attributes` is `attributes.from_metadata(self.event_metadata({}))`.

---

## 5. Observation

```py
class Component:
    def event_metadata(self, metadata: Mapping[str, Any], window: PartitionWindow | None = None, **extra) -> dict[str, Any]
        # {**metadata, component_id, component_kind, component_key, qualified_key, window, [parent_id], **extra}

class StepKind(Enum):                   # interloper.events.step
    ASSET_DATA = "asset_data"
    DEST_READ = "dest_read"
    DEST_WRITE = "dest_write"
    started, completed, failed -> EventType      # EventType(f"{value}_{phase}")
    span -> str                                  # "interloper.asset.data", "interloper.destination.read", "interloper.destination.write"
    def message(self, phase, subject, error=None) -> str

class Step:                             # context manager; one triplet around one span
    def __init__(self, kind: StepKind, metadata: dict[str, Any], *, subject: str, attributes: dict[str, str] | None = None)
    def record(self, **extra) -> None   # extra metadata on the completed event
    __enter__ -> Step                   # emits started, opens the span with attributes.from_metadata(metadata) plus attributes
    __exit__                            # emits completed with the recorded extra, or failed with error and traceback, then re-raises

class EventLogger:
    def __init__(self, component: Component, metadata: Mapping[str, Any], window: PartitionWindow | None = None)
```

- `event_metadata` always carries `window` (a string, or `None`), so every event has the same
  shape; `parent_id` is present only when the component has a parent. `qualified_key` is carried
  for every component, which is what lets the `Asset` override go.
- `StepKind.message` keeps today's wording in one table: `Executing {subject}` / `Executed
  {subject}` / `Execution of {subject} failed: {error}`; `Reading` / `Read` / `Failed to read
  {subject}: {error}`; `Writing` / `Wrote` / `Failed to write {subject}: {error}`.
- `Step.__exit__` formats the error through `format_exception` and attaches
  `traceback.format_exc()`; the span's own status is set by the tracer's context manager.
- The asset's three triplets become `Step` blocks: the data call with
  `StepKind.ASSET_DATA`, the write with `StepKind.DEST_WRITE` and `destination_key` in the
  metadata and `rows` recorded, the read with `StepKind.DEST_READ`, `destination_key` in the
  metadata and the upstream key as a span attribute.
- `RunState` keeps its operation transitions and deterministic ids in this phase and builds
  metadata through `operation.event_metadata(self.metadata, self.window, attempt=...)`. Its
  run-level events carry `window` under the same key.
- `telemetry.attributes._METADATA_KEYS` maps `window` to the unchanged `interloper.partition`
  attribute.
- `EventLogger._emit` builds through `component.event_metadata(metadata, window, level=..., message=...)`,
  so a log line inside a partitioned run carries the window like every other event. It is
  constructed only by `ExecutionContext.logger`, which hands it the asset, the metadata and the window.

---

## 6. Runner, state, result

- `Runner.run(dag, window: Partition | PartitionWindow | None = None, metadata=None)`
  normalises through `PartitionWindow.of` and passes the window down.
- `_preflight(dag, window)`: for every enabled operation with a partitioning, call
  `partitioning.check(window)`; collect the messages, prefixed with the operation key, and raise
  one `PartitionError` listing them. An operation without partitioning is not checked.
- `RunState.start_run(window)`, `RunState.window`; `RunResult.window`, `__str__` prints
  `window=<str>` or `window=None`.
- `_submit_operation(operation, window)` on every runner, including Docker and Kubernetes;
  `_worker(operation_id, dag_spec, window, metadata)`.
- The Docker and Kubernetes runners drop `_handle_flushed_future` and `_cancel_all`, which nothing
  calls.
- The scheduler executor keeps passing a `TimePartition`; the runner's sugar accepts it.

---

## 7. Errors

- `PartitionError(ConfigError)`. Raised by `check`, `partition`, `window`, `from_key`, the
  runner preflight and `Asset.run_async`.
- The `run` CLI keeps catching `PartitionError` for its message.

---

## 8. Out of scope, follow-ups

- Splitting `Asset` by concern, lazy cascade from `Source`: phase 2.
- The runner walk and attempt loops, moving the operation lifecycle onto `Step`: phase 3.
- Platform support for list partitioning, one ticket in ITLPR: backfill expansion through
  `PartitionConfig.window` instead of `TimeGranularity.period_range`, the run form offering list
  keys, the coverage view, and the app's partitioning readers, which today treat a partitioning
  without a granularity as daily.
- Renaming the partitioning types to plainer names (`Partitioning`, `Granularity`) and the CLI
  flags: the surface sweep, phase 5.

---

## 9. Testing

Mirror layout, red then green per unit. Core:

- `tests/partitioning/test_base.py`: the abstract contracts through the list kind; `of`,
  `__str__`, `filter`, `from_definition` dispatch.
- `tests/partitioning/test_time.py`: `check`, `partition`, `window`, `compatible`, `bounds`,
  `filter`, chronological `partitions`, `__len__`, removed members absent.
- `tests/partitioning/test_list.py`: the list kind end to end.
- `tests/asset/test_context.py`: the new constructor, `partition`, `window`, `partition_date`,
  `logger` for time and list partitionings.
- `tests/asset/test_base.py`: entry points with a partition and a window, the check at run time,
  the three steps' events, a windowed upstream read arriving as one table, a list-partitioned
  asset materialising and being read back.
- `tests/destination/test_context.py`, `test_base.py`, `test_database.py`: `partitions`,
  `slices`, one-table reads, the single write path, filters for both kinds.
- `tests/representation/test_base.py`: `concat`.
- `tests/events/test_step.py`: both outcomes, messages, traceback, recorded extra, span attributes.
- `tests/events/test_logger.py`: the component-based logger.
- `tests/component/test_base.py`: `event_metadata`.
- `tests/operation/test_base.py`, `tests/runner/*`, `tests/dag/test_base.py`,
  `tests/cli/commands/test_run.py`, `tests/telemetry/test_attributes.py`: the renames and the
  preflight.

Outside core: interloper-pandas (`concat`), the destination packages' tests (`window=`, one-table
window reads), interloper-assets' tests (`ExecutionContext(asset, window)`), the db store's
partitioning constructor and its tests, the Docker and Kubernetes command builders and their
tests, the scheduler's tests that touch `RunResult`.

Docs: `guide/assets.md`, `guide/partitioning.md`, `guide/destinations.md`, `guide/execution.md`,
`guide/events.md`, `guide/dependencies.md`, the generated reference.
