# Core phase 5: the surface sweep

Date: 2026-10-14. Status: approved in conversation, implementation pending.

Phase 5 of the interloper-core consolidation programme (five stacked phases: scope, contexts and
observation; Asset and Source; the runner family; Component, Serializable and decorators; the
surface sweep). Phases 1 to 4 reshaped the kernel; this phase settles its surface: a short list of
public renames where one concept had two names, imports through the package surface, dead code
removed, the minors phases 1 to 4 deferred, internal names and stale text, and backfills emitting
their lifecycle events.

Scope: the kernel packages of `interloper-core` (asset, source, component, serializable, dag,
runner, operation, events, partitioning, destination, connection, config, schema, normalizer,
representation, job, hook, catalog, resource, retry, errors, registry, utils), the Docker and
Kubernetes runners, the import sites in every workspace package, the backfill store and its
scheduler wiring in `interloper-db`, and the docs, skills and examples. The periphery (`rest`,
`oauth`, `telemetry`, `cli`, `settings`) is phase 6. Built on phase 4 (PR #461).

Rename policy (decided): a curated set of author-facing renames, each with an upgrade row; nothing
persisted changes. Event type values, runner config keys, classpaths and spec shapes stay.

---

## 1. Curated public renames

### 1.1 A partition is identified by its `key`

`Partition.id`, `TimePartition.id` and `ListPartition.id` become `.key`, matching
`TimePartition.from_key`, `PartitionConfig.partition(key)`, `TimeGranularity.key_format`, the
partition's own docstring ("the partition's key") and `partition_key` everywhere outside core.
Every reader follows: `window.first.key`, `window.last.key`, the container runners' window flags,
the ClickHouse destination, docs and skills. The values are unchanged.

### 1.2 `list[dict]` is `rows`

`Upstream.records` becomes `Upstream.rows`; `Representation.to_records` / `from_records` become
`to_rows` / `from_rows`, in core and in every implementation (pandas, sql, google-cloud, azure);
the representation wrapper's `.records` becomes `.rows`. "rows" is the house word (the rows
representation's key, `Schema.validate_rows`, 188 uses against 57).

### 1.3 One class per name

`interloper.representation.View`, a representation bound to its data, becomes `TableData`, and its
construction moves onto it: `TableData.from_data(data)` replaces `Representation.of(data)`. The
`View` asset kind is untouched. `TableData` keeps every member of today's class (`representation`,
`data`, `key`, `rows`, `columns`, `to(key)`, ...), renamed only where 1.2 renames.

### 1.4 Run kinds named after their runner

`InProcessRun`, `ContainerRun` and `JobRun` become `AsyncRun`, `DockerRun` and `KubernetesRun`:
the run kind of `AsyncRunner`, `DockerRunner` and `KubernetesRunner`. `JobRun` no longer reads as
a run of a `Job`.

### 1.5 Not renamed

The runner capacity names (`max_workers`, `max_containers`, `max_jobs`) stay as they are.
`Asset.run` / `materialize` stay (compute without writing, compute and write).
`ExecutionContext` / `IOContext`, the `dest_*` event values and module locations stay.

---

## 2. The export surface

- Every workspace package imports kernel names through the package that exports them
  (`from interloper.catalog import Catalog`, `from interloper.resource import InputField`,
  `from interloper.partitioning import TimePartition`, `from interloper.runner import Run`, ...),
  never through a module inside it. About 90 import sites change. Imports between modules of one
  core package, and spec `path:` strings (a classpath is identity), are unchanged.
- What a package's importers need, it exports: `CronField` and `TimezoneField` from
  `interloper.resource` (the top-level `__init__` stops reaching past it); `json_default` and
  `replace_non_finite` from `interloper.utils`.
- `PartitionFilter` has one home, `interloper.partitioning`; `interloper.destination` stops
  re-exporting it, and its 8 backends and 2 tests import it from `interloper.partitioning`.
- Names only their own module uses become private: `RECONCILERS`, `FETCH_FIELD_PROVIDER_ATTR`,
  `HookEvent`. `Compilable` is no longer exported from `interloper.destination`.
- Unchanged, deliberately: the top-level decorator names shadowing their subpackages (the
  decorators are the author API; `from interloper.asset import ...` keeps working); the hook event
  spellings (`HookEvent`, `HOOK_EVENT_TYPES`, `EventType`), which serve the form, the runtime check
  and the event; the `ListPartition` / `ListPartitionWindow` exports, which mirror `Time*`.

---

## 3. Dead code and deferred fixes

### 3.1 Removed

- `SourceError` (never raised, caught or referenced).
- `utils.to_slug_case` and `utils.require_import` (no caller beyond their own tests).
- `Event.from_log_line`'s bare-JSON branch ("legacy / testing convenience"; every producer writes
  the `@EVENT:` prefix).
- Parameters no caller passes: `Catalog.get(default=)`, `TimeGranularity.period_range(reversed=)`
  (test-only, shadows a builtin), `import_from_path(target_type=)` (test-only, with its typed
  overload), `Schema.infer(name=)` (test-only).
- The field factories' `title=` passthrough: `label=` is the one spelling; a factory given
  `title=` raises `TypeError` naming `label=`. `CronJob`'s `cron` and `timezone` fields use
  `label=`. Plain pydantic `Field(title=...)` is unaffected.
- Duplicates: the Databricks destination's hand-rolled `_Lock` becomes `interloper.utils.RLock`;
  the component-key and message fallback written in both `Event.__str__` and the console handler's
  formatter is written once, on `Event`.

Kept, deliberately: `TextField`, `DAG.get_predecessors` / `get_successors`, `ExecutionInfo.to_dict`
(documented author API); `MaterializationStrategy`'s `"auto"` alias and the `k8s` runner alias
(persisted in configs and the chart).

### 3.2 Backfills emit their lifecycle

`EventType.BACKFILL_STARTED`, `BACKFILL_COMPLETED` and `BACKFILL_FAILED` exist and nothing emits
them as events today (the latter two are hook trigger names only). The backfill store records them
at its transitions, in the same transaction:

- `BACKFILL_STARTED` when `BackfillStore.create` creates the backfill (which starts it: its status
  is `running` and `started_at` is set there);
- `BACKFILL_COMPLETED` or `BACKFILL_FAILED` where `_advance_in` finalizes it, on the normal end and
  on the fail-fast end.

Each event is about the backfill's target component (`component_id`, `component_kind`,
`component_key`), carries `backfill_id` and the backfill's partition bounds in its metadata, has no
`run_id`, and has a deterministic id (uuid5 of the backfill id and the event type) so a retried
transition never records it twice. The backfill store holds the event store, as the run store
does. Hooks keep firing from the backfill status poll, unchanged. Canceling a backfill records
nothing: there is no event type for it, and adding one is a new contract.

### 3.3 Fixes deferred by phases 1 to 4

Each is pinned by a test that fails first.

- Runtime help: `Decorator` takes `doc=` and sets the instance's `__doc__`, so `help(il.asset)`
  and `il.asset?` show the authored text, which has one source. The docs reference renders it by
  inspection (`force_inspection` on those entries); if the docs build cannot, a per-kind
  `Decorator` subclass carries the docstring instead.
- Refusing an asset another source owns says what to do: give the source the asset's overrides
  through `assets={key: {...}}`.
- The settings refusal's accepted list leaves out `id`, and its hint reads "it is a setting of
  `X`" (not "subclass it", wrong for `@il.asset(dialect=...)`).
- A source function returning anything but asset classes raises `TypeError` naming what it
  returned.
- `Run.walk`'s drain runs under the same fault handling as scheduling, and recording tolerates a
  canceled task: a fault while draining still ends with the pending operations canceled and
  `RUN_FAILED`.
- With `reraise`, `Runner.run` raises inside the run's span, so the trace records the exception.
- Missing tests: `operation_started` on attempt 2 through `AsyncRunner`, with its own event id; no
  event id repeated in a fail-fast run.
- `source(assets=...)` through `Component.__call__` is refused (`TypeError`): it would bypass the
  ownership wiring. Overrides go through the constructor's `assets={key: {...}}` map.
- `Upstream.read`'s missing-data warning names the parameter again.
- `ListPartitionConfig(values="eu")` is refused instead of read as `("e", "u")`.
- `PartitionConfig.from_definition` given an unknown kind raises `ConfigError`.
- The list-kind check and the partition-dependency messages read as sentences about the asset.

Left as is: closing the Docker client per run (shared with polling threads that outlive a canceled
run), a Kubernetes config error raising from `run` (phase 3's upgrade notes), `copy.deepcopy` of a
source (no caller), the CLI `--date` help text (phase 6).

---

## 4. Internal names, stale text, examples

- Abbreviated locals and private names become words (about 60: `k`/`v`, `e`, `inst`, `preds`,
  `kids`, `coro`, `fmt`, `defn`, `span_attrs`, `runner_cls`, `asset_cls`, `config_cls`,
  `_OPERATION_EVENT_NS`, `_ENV_VAR_RE`, `_KEY_RE`, ...).
- Private helpers named `resolve` that do something else take the verb for it:
  `DAG._resolve_declared` becomes `_bind_declared`, `Schema._resolve_field_type` becomes
  `_widen_types`. The public `resolve=` callback and `Component.resolve` stay.
- Section dividers use the house titles (`Helpers` becomes `Internals`, `Constructors` becomes
  `Construction`, ...).
- Stale docstrings name what exists: `AssetRef` (now `AssetMeta`), `_read_upstreams`,
  `Source._resolve`, `children` (now `owned`), `.conformer`, `validate` (now `validate_rows`),
  `target` (now `targets`), `source_id` (now `parent_id`), the composite classpath format, and the
  phase 4 leftovers (`Component._collect`, `InheritedSetting.install`, the `trickle_resources`
  upgrade row).
- Docs: `RunResult(... partition=...)` shows `window=`; `guide/destinations.md` stops claiming a
  `DestinationError`; the `"auto"` strategy alias names one version.
- Examples drive coroutines with `il.run(...)`, the documented sync bridge, instead of
  `asyncio.run(...)`.

---

## 5. Behaviour changes

Listed in the upgrade skill's row for this phase:

- `Partition.id` is `Partition.key` (`window.first.key`, ...).
- `Upstream.records` is `Upstream.rows`; `Representation.to_records` / `from_records` are
  `to_rows` / `from_rows`.
- `interloper.representation.View` is `TableData`; `Representation.of(data)` is
  `TableData.from_data(data)`.
- `InProcessRun`, `ContainerRun`, `JobRun` are `AsyncRun`, `DockerRun`, `KubernetesRun`.
- `PartitionFilter` is imported from `interloper.partitioning` only.
- `SourceError`, `utils.to_slug_case`, `utils.require_import` are removed; so are
  `Catalog.get(default=)`, `period_range(reversed=)`, `import_from_path(target_type=)`,
  `Schema.infer(name=)`.
- Field factories take `label=` only.
- `source(assets=...)` raises; give overrides at construction.
- Backfills record `backfill_started` and `backfill_completed` / `backfill_failed` events.

Unchanged: event type values, runner config keys (capacity names included), classpaths, spec
shapes, catalog keys, kinds and paths.

---

## 6. Testing

Mirror layout, red then green per unit.

- Each rename: the new name works and the old one is gone (a grep over sources, docs, skills and
  examples finds none; `ty` checks every package).
- Each removal and fix: a test that fails before the change (3.1, 3.3).
- Backfills: `tests/store/test_backfills.py` in `interloper-db` pins `BACKFILL_STARTED` on create,
  `BACKFILL_COMPLETED` / `BACKFILL_FAILED` on finalize (normal and fail-fast), the deterministic
  ids, and no event on cancel.
- The catalog snapshot before and after is identical.
- The strict docs build passes; `help(il.asset)` shows the authored text.
- Live: every local example runs; the demo manifest runs; a backfill through the scheduler records
  `backfill_started` then `backfill_completed`; a demo run through the API completes.
