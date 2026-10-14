# Core Phase 5: The Surface Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Settle interloper-core's surface: four curated public renames, imports through package surfaces, dead code removed, backfills emitting their lifecycle events, the minors phases 1 to 4 deferred, and internal names and stale text cleaned.

**Architecture:** Mostly mechanical renames and removals, each pinned by a test and by `ty` across the workspace; two behaviours change (backfill events in the db store, and the deferred fixes in the run walk, `Component.__call__`, partitioning and the decorator). Nothing persisted changes.

**Tech Stack:** Python 3.12 (floor 3.10), Pydantic 2.12, SQLModel, pytest, ruff, ty, zensical (docs), uv workspace.

**Spec:** `docs/superpowers/specs/2026-10-14-core-surface-design.md`

## Global Constraints

- No em dash character in any text: code, comments, docstrings, docs, messages, tests.
- Google-style docstrings, complete: every module, class, function and method, private helpers, `__init__` and dunders included; `Args:` lists every parameter, `Returns:`/`Raises:` whenever they apply (`Raises:` only for an explicit `raise` in the body). Product-copy docstrings (decorators, components) carry prose only.
- Beautiful shapes: no nested functions or closures as structure; logic on the class that owns its data.
- Names written in full; one word per concept, the house word.
- Comments only for a non-obvious why; no attribute-level comments; no comments narrating the change.
- Tests mirror the source layout one-to-one; no new `test_<feature>.py`.
- Run uv with `UV_FROZEN=1`; never change `uv.lock`.
- Never commit, amend or push during the tasks; the work rests uncommitted until the finish step.
- Nothing persisted changes: event type values, runner config keys (`max_workers`, `max_containers`, `max_jobs` included), classpaths, spec shapes, catalog keys, kinds and paths.
- No new concept an author has to learn.
- Ruff line length 120.
- Gate per task: `UV_FROZEN=1 make check-python` green at the end of every task.

## Review Focus

1. A destination keying storage by partition (`partition.key` in a path or a filter) must write and read exactly where it did before the rename. Pinned in Task 1 (`test_storage_paths_are_unchanged_by_the_rename`).
2. A backfill whose `create` transaction rolls back (a quota refusal) must record no `backfill_started` event. Pinned in Task 6 (`test_a_refused_backfill_records_no_event`).
3. A backfill finalized twice (a second `advance` racing the first) must record its terminal event once. Pinned in Task 6 (`test_the_terminal_event_is_recorded_once`).
4. A spec written before the `rows` / `TableData` renames must reconstruct unchanged (they touch no serialized field). Pinned in Task 2 (`test_a_spec_round_trips_through_the_rename` in `tests/asset/test_upstream.py`).
5. `help(il.asset)` and `il.asset?` show the authored text, and the docs reference still renders each decorator. Pinned in Task 7 (`test_a_decorator_carries_its_documentation`) and the docs-build step.

---

## File Structure

| Area | Files | Tasks |
|---|---|---|
| Partitions | `partitioning/{base,time,list}.py`, the destinations, both container runners, `interloper-clickhouse` | 1 |
| Representations | `representation/base.py`, `representation/__init__.py`, `asset/{upstream,output}.py`, destinations, `interloper-{pandas,sql,google-cloud,azure,databricks,clickhouse,snowflake,duckdb,assets}` | 2 |
| Run kinds | `runner/async_runner.py`, `runner/__init__.py`, `interloper_docker/runner.py`, `interloper_k8s/runner.py` | 3 |
| Export surface | every workspace package's imports; `resource/__init__.py`, `utils/__init__.py`, `destination/__init__.py`, `interloper/__init__.py` | 4 |
| Removals | `errors.py`, `utils/{text,imports}.py`, `events/{event,console}.py`, `catalog/base.py`, `partitioning/time.py`, `schema/base.py`, `resource/fields.py`, `job/cron.py`, `interloper-databricks` | 5 |
| Backfill events | `interloper-db` `store/{backfills,base}.py` | 6 |
| Deferred fixes | `component/decorator.py`, decorator modules, `serializable/base.py`, `component/base.py`, `source/base.py`, `runner/{run,base}.py`, `asset/{base,upstream}.py`, `interloper-sql` view, `partitioning/{base,list}.py`, `dag/base.py` | 7 |
| Names, text, docs | many | 8 |

---

### Task 1: A partition is identified by its key

**Files:**
- Modify: `packages/interloper-core/src/interloper/partitioning/base.py`, `time.py`, `list.py`; every reader `ty` reports
- Test: `packages/interloper-core/tests/partitioning/test_base.py`, `test_time.py`, `test_list.py`, `tests/destination/test_memory.py`

**Interfaces:**
- Produces: `Partition.key -> str` (abstract property on `Partition`, implemented by `TimePartition` and `ListPartition`); `Partition.id` no longer exists.

- [ ] **Step 1: Write the failing tests**

In `tests/partitioning/test_time.py` and `tests/partitioning/test_list.py`, add:

```python
def test_a_partition_is_identified_by_its_key():
    assert il.TimePartition(dt.date(2026, 1, 15)).key == "2026-01-15"
    assert not hasattr(il.TimePartition(dt.date(2026, 1, 15)), "id")
```

```python
def test_a_partition_is_identified_by_its_key():
    assert il.ListPartition("eu").key == "eu"
    assert not hasattr(il.ListPartition("eu"), "id")
```

In `tests/destination/test_memory.py`, add (Review Focus 1):

```python
def test_storage_paths_are_unchanged_by_the_rename():
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily() -> list[dict]:
        return [{"date": "2026-01-15", "x": 1}]

    il.MemoryDestination.clear()
    destination = il.MemoryDestination()
    il.DAG(daily(destinations=[destination])).materialize(il.TimePartition(dt.date(2026, 1, 15)))

    assert any("date=2026-01-15" in path for path in il.MemoryDestination._storage)
```

(`MemoryDestination._storage` is the class-level dict every instance writes to, keyed by the path `memory.py:98` builds from `partition.key`.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/partitioning -q -p no:cacheprovider -k key`
Expected: FAIL (`AttributeError: ... has no attribute 'key'`).

- [ ] **Step 3: Rename the definitions**

In `partitioning/base.py`, `time.py`, `list.py`: the abstract property and its two implementations `def id(self) -> str:` become `def key(self) -> str:` (docstring "The partition's key." kept). In the same modules, every `self.id`, `partition.id`, `first.id`, `last.id` on a partition becomes `.key`.

- [ ] **Step 4: Let `ty` find every reader**

Run: `UV_FROZEN=1 uv run ty check --output-format concise 2>&1 | grep -E "has no attribute .id."`
For each hit on a `Partition`, `TimePartition`, `ListPartition` or window member, change `.id` to `.key`. Expected readers: `destination/{csv,file,memory,object_store}.py`, `interloper_docker/runner.py:171,173`, `interloper_k8s/runner.py:253,255`, `interloper_clickhouse/destination.py:543`, and tests (`tests/asset/test_context.py`, `tests/asset/test_base.py`, `tests/runner/test_base.py`, `tests/destination/test_context.py`, `tests/destination/test_base.py`). Do not touch a component's `.id` (for example `tests/dag/test_base.py:658`).
Re-run until no partition `.id` remains. Then: `grep -rn "partition\.id\b\|first\.id\b\|last\.id\b" packages docs plugins examples --include='*.py' --include='*.md'` finds nothing.

- [ ] **Step 5: Docs and skills**

`grep -rn "\.id\b" docs plugins --include='*.md' | grep -i "partition\|window\.first\|window\.last"` and change each partition read to `.key`.

- [ ] **Step 6: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

- [ ] **Step 7: Checkpoint (no commit)**

---

### Task 2: Rows and TableData

**Files:**
- Modify: `packages/interloper-core/src/interloper/representation/base.py`, `representation/__init__.py`, `asset/upstream.py`, `asset/output.py`, `destination/{base,csv,database,object_store}.py`, `partitioning/{time,list}.py`; `interloper-pandas` (`representation.py`, `__init__.py`), `interloper-sql/destination.py`, `interloper-google-cloud` (`google_sheets/destination.py`, `bigquery/destination.py`), `interloper-azure/fabric/destination.py`, `interloper-databricks`, `interloper-clickhouse`, `interloper-snowflake`, `interloper-duckdb` destinations, `interloper-assets/campaign_matcher/source.py`
- Test: `tests/representation/test_base.py`, `tests/asset/test_upstream.py`, `interloper-pandas/tests/test_representation.py`

**Interfaces:**
- Produces: `TableData(representation, data)` (frozen dataclass, exported from `interloper.representation`) with `TableData.from_data(data) -> TableData`, `.key`, `.rows`, `.columns`, `.to(key)`, and today's other members; `Representation.to_rows(data)`, `Representation.from_rows(rows)`; `Upstream.rows`; `interloper_pandas.dataframe_to_rows`. Gone: `representation.View`, `Representation.of`, `to_records`, `from_records`, `Upstream.records`, `dataframe_to_records`.

- [ ] **Step 1: Write the failing tests**

In `tests/representation/test_base.py`:

```python
from interloper.representation import TableData


class TestTableData:
    def test_from_data_binds_the_matching_representation(self):
        table = TableData.from_data([{"a": 1}])

        assert table.key == "rows"
        assert table.rows == [{"a": 1}]
        assert table.columns == ["a"]

    def test_the_rows_representation_round_trips_rows(self):
        representation = TableData.from_data([{"a": 1}]).representation

        assert representation.from_rows(representation.to_rows([{"a": 1}])) == [{"a": 1}]
```

In `tests/asset/test_upstream.py`:

```python
class _Rows(il.Asset):
    """An asset handing rows over."""

    def data(self) -> list[dict[str, Any]]:
        return [{"a": 1}]


class _Shop(il.Source):
    """A source owning one asset."""

    asset_types: ClassVar[list[type[il.Asset]]] = [_Rows]


def test_rows_reads_the_upstream_data_as_rows():
    upstream = Upstream(asset=_Rows(), data=[{"a": 1}])

    assert upstream.rows == [{"a": 1}]
    assert not hasattr(upstream, "records")


def test_a_spec_round_trips_through_the_rename():
    source = _Shop()

    assert il.Source.from_spec(source.to_spec()).to_spec() == source.to_spec()
```

(`ClassVar` from `typing`; `Upstream` is already imported in this file.)

In `packages/interloper-pandas/tests/test_representation.py`: replace `to_records`/`from_records`/`dataframe_to_records` in the existing tests by `to_rows`/`from_rows`/`dataframe_to_rows` (they then fail until Step 3).

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/representation packages/interloper-core/tests/asset/test_upstream.py packages/interloper-pandas -q -p no:cacheprovider`
Expected: FAIL (`ImportError: cannot import name 'TableData'`, `to_rows` missing).

- [ ] **Step 3: Rename in core**

In `representation/base.py`:
- `class View` becomes `class TableData`; its docstring opens "A representation bound to the data it holds."; `records` property becomes `rows` (calls `self.representation.to_rows(self.data)`); the conversion method's `target.from_records(self.records)` becomes `target.from_rows(self.rows)`.
- `Representation.to_records` / `from_records` (abstract and the rows implementation) become `to_rows` / `from_rows`.
- `Representation.of(data)` is removed; its body becomes the classmethod `TableData.from_data(cls, data: Any) -> TableData` with the same lookup and the same error message ("No representation matches ..."), docstring:

```python
    @classmethod
    def from_data(cls, data: Any) -> TableData:
        """Bind data to the registered representation that recognises it.

        Args:
            data: A table in any registered representation.

        Returns:
            The data with its representation; rows when the data is ``list[dict]``.

        Raises:
            RepresentationError: If no registered representation matches the
                data (the message `Representation.of` raised today, unchanged).
        """
```

- The module docstring's mention of ``Representation.of(data)`` becomes ``TableData.from_data(data)``.
- `representation/__init__.py` exports `TableData` instead of `View`.

Every `Representation.of(x)` in the workspace becomes `TableData.from_data(x)` (import `TableData` from `interloper.representation`; drop `Representation` from the import where it becomes unused), and every `.records` on its result becomes `.rows`. `Upstream.records` becomes `Upstream.rows`. `interloper_assets/campaign_matcher/source.py:104` reads `.rows`.

- [ ] **Step 4: Rename the implementations**

`interloper_pandas/representation.py`: `to_records` / `from_records` become `to_rows` / `from_rows`; the module function `dataframe_to_records` becomes `dataframe_to_rows` (and in `interloper_pandas/__init__.py` and its `__all__`). Any other `Representation` subclass `ty` reports as no longer implementing the abstract methods gets the same rename.

- [ ] **Step 5: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages -q -p no:cacheprovider > .superpowers/t2.log 2>&1; tail -2 .superpowers/t2.log`
Expected: PASS.
Run: `grep -rnw "to_records\|from_records\|dataframe_to_records\|Representation\.of" packages docs plugins examples --include='*.py' --include='*.md' | grep -v docs/superpowers`
Expected: no output. `grep -rn "\.records\b" packages --include='*.py'` finds no representation or upstream read.

- [ ] **Step 6: Docs**

Docs mentioning `Upstream.records`, `view.records`, `Representation.of`, `to_records`, `from_records` or `representation.View` are rewritten to the new names (`grep -rn "records\|Representation.of" docs plugins --include='*.md'`).

- [ ] **Step 7: Full check**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

- [ ] **Step 8: Checkpoint (no commit)**

---

### Task 3: Run kinds named after their runner

**Files:**
- Modify: `packages/interloper-core/src/interloper/runner/async_runner.py`, `runner/__init__.py`, `packages/interloper-docker/src/interloper_docker/runner.py`, `packages/interloper-k8s/src/interloper_k8s/runner.py`, their tests, `docs/extending/runners.md`
- Test: `tests/runner/test_async_runner.py`, `interloper-docker/tests/test_runner.py`, `interloper-k8s/tests/test_runner.py`

**Interfaces:**
- Produces: `AsyncRun` (exported from `interloper.runner`), `DockerRun`, `KubernetesRun`; `InProcessRun`, `ContainerRun`, `JobRun` no longer exist.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_run_kind_is_named_after_its_runner():
    from interloper.runner import AsyncRun

    assert il.AsyncRunner.Run is AsyncRun
```

(in `tests/runner/test_async_runner.py`), and the same shape in the Docker (`DockerRunner.Run is DockerRun`) and Kubernetes (`KubernetesRunner.Run is KubernetesRun`) runner tests.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/runner packages/interloper-docker packages/interloper-k8s -q -p no:cacheprovider -k "named_after"`
Expected: FAIL (import errors).

- [ ] **Step 3: Rename**

`InProcessRun` becomes `AsyncRun` (class, `AsyncRunner.Run`, `runner/__init__.py` import and `__all__`, docstrings, tests); `ContainerRun` becomes `DockerRun`; `JobRun` becomes `KubernetesRun` (classes, `Run =` assignments, annotations, docstrings, tests); `docs/extending/runners.md` uses `AsyncRun` where it names the in-process run.

- [ ] **Step 4: Run the tests and grep**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/runner packages/interloper-docker packages/interloper-k8s -q -p no:cacheprovider`
Expected: PASS. `grep -rnw "InProcessRun\|ContainerRun\|JobRun" packages docs plugins examples | grep -v docs/superpowers` prints nothing.

- [ ] **Step 5: Full check, checkpoint (no commit)**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

---

### Task 4: The export surface

**Files:**
- Modify: `packages/interloper-core/src/interloper/resource/__init__.py`, `utils/__init__.py`, `destination/__init__.py`, `interloper/__init__.py`, `schema/base.py`, `resource/fields.py`, `hook/base.py`; import sites in every workspace package (src and tests)
- Test: `packages/interloper-core/tests/test_package.py` if it exists, else `tests/resource/test_fields.py` and `tests/utils/test_json.py`

**Interfaces:**
- Produces: `interloper.resource` exports `CronField`, `TimezoneField`; `interloper.utils` exports `json_default`, `replace_non_finite`; `interloper.destination` no longer exports `PartitionFilter` or `Compilable`; `_RECONCILERS`, `_FETCH_FIELD_PROVIDER_ATTR`, `_HookEvent` private.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_resource_package_exports_every_field_factory():
    from interloper.resource import CronField, TimezoneField

    assert callable(CronField) and callable(TimezoneField)
```

(in `tests/resource/test_fields.py`), and

```python
def test_the_utils_package_exports_the_json_helpers():
    from interloper.utils import json_default, replace_non_finite

    assert callable(json_default) and callable(replace_non_finite)
```

(in `tests/utils/test_json.py`), and in `tests/destination/test_base.py`:

```python
def test_partition_filter_has_one_home():
    import interloper.destination as destination

    assert not hasattr(destination, "PartitionFilter")
    assert not hasattr(destination, "Compilable")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/resource packages/interloper-core/tests/utils packages/interloper-core/tests/destination/test_base.py -q -p no:cacheprovider -k "exports or one_home"`
Expected: FAIL.

- [ ] **Step 3: Exports**

- `resource/__init__.py` imports and lists `CronField`, `TimezoneField`; `interloper/__init__.py` imports them from `interloper.resource`.
- `utils/__init__.py` imports and lists `json_default`, `replace_non_finite` from `interloper.utils.json`.
- `destination/__init__.py` stops importing and listing `PartitionFilter` and `Compilable`.
- `RECONCILERS` (schema/base.py) becomes `_RECONCILERS`, `FETCH_FIELD_PROVIDER_ATTR` (resource/fields.py) becomes `_FETCH_FIELD_PROVIDER_ATTR`, `HookEvent` (hook/base.py) becomes `_HookEvent`, with every use in their modules (and in tests, if any reference them).

- [ ] **Step 4: Imports through package surfaces**

Rewrite every import from OTHER workspace packages (src and tests) that names a module inside a kernel package to import from the package. The mapping (module path to package):

| Imported from | Import from |
|---|---|
| `interloper.catalog.base` | `interloper.catalog` |
| `interloper.connection.base` | `interloper.connection` |
| `interloper.job.cron` | `interloper.job` |
| `interloper.dag.base` | `interloper.dag` |
| `interloper.component.decorator` | `interloper.component` |
| `interloper.resource.fields` | `interloper.resource` |
| `interloper.partitioning.base`, `interloper.partitioning.time` | `interloper.partitioning` |
| `interloper.runner.base`, `.run`, `.state` | `interloper.runner` |
| `interloper.source.base` | `interloper.source` |
| `interloper.destination.database` | `interloper.destination` |
| `interloper.utils.concurrency`, `.time`, `.imports`, `.data`, `.json` | `interloper.utils` |
| `interloper.destination` (for `PartitionFilter`) | `interloper.partitioning` |

Find them with: `grep -rnE "from interloper\.(catalog|connection|job|dag|component|resource|partitioning|runner|source|destination|utils)\.[a-z_]+ import" packages --include='*.py' | grep -v "packages/interloper-core/src/"` plus `grep -rn "PartitionFilter" packages --include='*.py'`. A name the package does not export yet (a `ty` unresolved-import after the rewrite) is added to that package's `__init__.py` and `__all__`. Imports inside `interloper-core/src` stay as they are (sibling modules of one package import each other directly; the top-level `__init__` changes only for `CronField`/`TimezoneField`). Spec `path:` strings in YAML and docs are not imports and stay.

- [ ] **Step 5: Verify**

Run the grep from Step 4: no output except imports inside `interloper-core/src`.
Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

- [ ] **Step 6: Checkpoint (no commit)**

---

### Task 5: Removals

**Files:**
- Modify: `packages/interloper-core/src/interloper/errors.py`, `utils/text.py`, `utils/imports.py`, `utils/__init__.py`, `events/event.py`, `events/console.py`, `catalog/base.py`, `partitioning/time.py`, `schema/base.py`, `resource/fields.py`, `job/cron.py`, `packages/interloper-databricks/src/interloper_databricks/destination.py`
- Test: `tests/test_errors.py`, `tests/utils/test_text.py`, `tests/utils/test_imports.py`, `tests/events/test_event.py`, `tests/events/test_console.py`, `tests/catalog/test_base.py`, `tests/partitioning/test_time.py`, `tests/schema/test_base.py`, `tests/resource/test_fields.py`

**Interfaces:**
- Produces: `Event.subject -> str` (qualified key, else component key, else `"-"`) and `Event.summary -> str` (message, else error, else `"-"`), read by `Event.__str__` and the console formatter. Gone: `SourceError`, `to_slug_case`, `require_import`, the bare-JSON branch, `Catalog.get(default=)`, `period_range(reversed=)`, `import_from_path(target_type=)` and its typed overload, `Schema.infer(name=)`, factories' `title=`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/events/test_event.py
def test_a_bare_json_line_is_not_an_event():
    event = Event(type=EventType.RUN_STARTED, metadata={"run_id": "r1"})

    assert Event.from_log_line(event.to_json()) is None


def test_subject_and_summary_fall_back_in_order():
    event = Event(type=EventType.LOG, metadata={"component_key": "orders", "error": "boom"})

    assert (event.subject, event.summary) == ("orders", "boom")
    assert (Event(type=EventType.LOG).subject, Event(type=EventType.LOG).summary) == ("-", "-")
```

```python
# tests/resource/test_fields.py
def test_a_field_factory_takes_label_not_title():
    with pytest.raises(TypeError, match=r"label="):
        il.InputField(title="Name")
```

```python
# tests/test_errors.py
def test_source_error_is_gone():
    import interloper.errors as errors

    assert not hasattr(errors, "SourceError")
```

```python
# tests/catalog/test_base.py
def test_get_returns_none_for_an_unknown_key():
    assert il.Catalog.discover().get("no_such_component") is None
```

The existing tests of `to_slug_case` (`tests/utils/test_text.py`), `require_import` (`tests/utils/test_imports.py`), `period_range(reversed=True)` (`tests/partitioning/test_time.py:143`), `import_from_path(..., target_type)` (`tests/utils/test_imports.py:49,53`), `Schema.infer(name=)` (`tests/schema/test_base.py:232`) and the bare-JSON event (`tests/events/test_event.py:156`) are deleted with the behaviour they pin.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/events packages/interloper-core/tests/resource packages/interloper-core/tests/test_errors.py -q -p no:cacheprovider -k "bare_json or subject or label_not_title or source_error"`
Expected: FAIL.

- [ ] **Step 3: Remove and fold**

- `errors.py`: delete `SourceError`.
- `utils/text.py`: delete `to_slug_case` (and its `utils/__init__.py` export); `utils/imports.py`: delete `require_import` and the `C` TypeVar if it becomes unused; `import_from_path` keeps one signature `(path: str) -> Any` (overloads and `target_type` deleted, `Raises: ValueError` dropped).
- `events/event.py`: `from_log_line` reads only `@EVENT:` lines (an unprefixed line returns `None`; docstring describes the one format). Add:

```python
    @property
    def subject(self) -> str:
        """What the event is about: its qualified key, else its component key, else ``"-"``."""
        return self.metadata.get("qualified_key") or self.metadata.get("component_key") or "-"

    @property
    def summary(self) -> str:
        """What the event says: its message, else its error, else ``"-"``."""
        return self.metadata.get("message") or self.metadata.get("error") or "-"
```

  `__str__` and `events/console.py`'s formatter read `event.subject` / `event.summary` instead of recomputing them.
- `catalog/base.py`: `get(self, key: str) -> ComponentDefinition | None`, returning `None` where it returned `default`.
- `partitioning/time.py`: `period_range(self, start, end)` without `reversed`.
- `schema/base.py`: `infer(cls, data)` (or its current other parameters) without `name`; the model is named `"InferredSchema"`.
- `resource/fields.py`: `_extra` raises `TypeError("Field factories take label=, not title=")` when `kwargs` holds `title`; its docstring drops "`title=` keeps working". `job/cron.py`'s `CronField(title=...)` and `TimezoneField(title=...)` become `label=`.
- `interloper_databricks/destination.py`: delete `_Lock`; the private attribute is `_lock: RLock = PrivateAttr(default_factory=RLock)` with `from interloper.utils import RLock`.

- [ ] **Step 4: Run the tests and grep**

Run: `UV_FROZEN=1 uv run pytest packages -q -p no:cacheprovider > .superpowers/t5.log 2>&1; tail -2 .superpowers/t5.log`
Expected: PASS.
Run: `grep -rnw "SourceError\|to_slug_case\|require_import" packages docs plugins examples | grep -v docs/superpowers`
Expected: nothing.

- [ ] **Step 5: Full check, checkpoint (no commit)**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

---

### Task 6: Backfills emit their lifecycle

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/backfills.py`, `packages/interloper-db/src/interloper_db/store/base.py`, `docs/guide/events.md`, `packages/interloper-core/src/interloper/events/types.py` (its docstring)
- Test: `packages/interloper-db/tests/store/test_backfills.py`

**Interfaces:**
- Produces: `BackfillStore(engine, quotas, runs, events)`; events `backfill_started` (on create), `backfill_completed` / `backfill_failed` (on finalize), each with `component_id`/`component_kind`/`component_key` of the target, `backfill_id`, `start_key`, `end_key`, `message`, no `run_id`, and id `uuid5(_EVENT_NAMESPACE, f"{backfill_id}:{event_type}")`.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-db/tests/store/test_backfills.py`, which already defines the `store` fixture, `_ORG_ID`, `_component`, `_backfill` and `_run_statuses`:

```python
def _lifecycle(store: Store, backfill_id: UUID) -> list[Event]:
    """The backfill's lifecycle events, oldest first.

    Args:
        store: The store under test.
        backfill_id: The backfill whose events are read.

    Returns:
        The events carrying its id.
    """
    with Session(store.engine) as session:
        events = session.exec(select(Event).order_by(col(Event.timestamp).asc())).all()
        return [event for event in events if (event.data or {}).get("backfill_id") == str(backfill_id)]


class TestLifecycleEvents:
    """A backfill records when it starts and how it ends."""

    def test_creating_a_backfill_records_that_it_started(self, store: Store):
        component_id = _component(store, kind="job")
        backfill = store.backfills.create(
            _ORG_ID, component_id=component_id, start_key="2026-01-01", end_key="2026-01-02"
        )

        [started] = _lifecycle(store, backfill.id)
        assert started.event_type == "backfill_started"
        assert (started.component_id, started.component_key, started.run_id) == (component_id, "job", None)
        assert (started.data["start_key"], started.data["end_key"]) == ("2026-01-01", "2026-01-02")

    def test_finishing_records_completed(self, store: Store):
        backfill = _backfill(store, days=2, concurrency=2)
        for run_id in _run_statuses(store, backfill.id):
            store.runs.complete(run_id, success=True)

        assert [event.event_type for event in _lifecycle(store, backfill.id)] == [
            "backfill_started",
            "backfill_completed",
        ]

    def test_a_failure_records_failed(self, store: Store):
        backfill = _backfill(store, days=2, concurrency=2)
        run_ids = list(_run_statuses(store, backfill.id))
        store.runs.complete(run_ids[0], success=False)
        store.runs.complete(run_ids[1], success=True)

        assert [event.event_type for event in _lifecycle(store, backfill.id)][-1] == "backfill_failed"

    def test_a_fail_fast_failure_records_failed(self, store: Store):
        backfill = store.backfills.create(
            _ORG_ID,
            component_id=_component(store, kind="job"),
            start_key="2026-01-01",
            end_key="2026-01-03",
            concurrency=1,
            fail_fast=True,
        )
        first = next(run_id for run_id, status in _run_statuses(store, backfill.id).items() if status == "queued")
        store.runs.complete(first, success=False)

        assert [event.event_type for event in _lifecycle(store, backfill.id)] == [
            "backfill_started",
            "backfill_failed",
        ]

    def test_the_terminal_event_is_recorded_once(self, store: Store):
        backfill = _backfill(store, days=1, concurrency=1)
        for run_id in _run_statuses(store, backfill.id):
            store.runs.complete(run_id, success=True)

        store.backfills.advance(backfill.id, failed=False)

        assert [event.event_type for event in _lifecycle(store, backfill.id)].count("backfill_completed") == 1

    def test_a_refused_backfill_records_no_event(self, store: Store, monkeypatch: pytest.MonkeyPatch):
        def refuse(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("refused")

        monkeypatch.setattr("interloper_db.store.backfills.save", refuse)
        with pytest.raises(RuntimeError, match="refused"):
            _backfill(store, days=1)

        with Session(store.engine) as session:
            assert session.exec(select(Event).where(Event.event_type == "backfill_started")).all() == []

    def test_canceling_records_nothing(self, store: Store):
        backfill = _backfill(store, days=2)
        store.backfills.cancel(backfill.id)

        assert [event.event_type for event in _lifecycle(store, backfill.id)] == ["backfill_started"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/store/test_backfills.py -q -p no:cacheprovider -k Lifecycle`
Expected: FAIL (no backfill events).

- [ ] **Step 3: Implement**

`store/base.py`: build `self.events = EventStore(self._engine)` before the backfill store, and pass it: `BackfillStore(self._engine, self.quotas, lambda: self.runs, self.events)`.

`store/backfills.py`:

```python
_EVENT_NAMESPACE = uuid.UUID("1b2f8c6a-48bd-4f2d-8950-27b9f7ac678d")
```

`__init__` takes `events: EventStore` (documented: "Event facet a backfill's lifecycle is recorded through") and keeps it as `self._events`. A private method records one transition, inside the caller's session (nested store calls share it, so the event commits or rolls back with the transition):

```python
    def _record(self, db_backfill: Backfill, event_type: il.EventType, message: str) -> None:
        """Record a lifecycle event of a backfill, once.

        Args:
            db_backfill: The backfill, its target component loadable.
            event_type: ``BACKFILL_STARTED``, ``BACKFILL_COMPLETED`` or ``BACKFILL_FAILED``.
            message: The event's message.
        """
        target = db_backfill.target
        metadata = {
            "component_id": str(target.id),
            "component_kind": target.kind,
            "component_key": target.key,
            "backfill_id": str(db_backfill.id),
            "start_key": db_backfill.start_key,
            "end_key": db_backfill.end_key,
            "message": message,
        }
        event_id = str(uuid.uuid5(_EVENT_NAMESPACE, f"{db_backfill.id}:{event_type.value}"))
        self._events.save(il.Event(type=event_type, metadata=metadata, id=event_id), db_backfill.org_id)
```

- In `create`, after `self._create_runs(...)` and before `save(session, db_backfill, "target")`: `self._record(db_backfill, il.EventType.BACKFILL_STARTED, f"Backfill of '{target.key}' started ({span} partitions)")`. `db_backfill.target` loads from the session, which already holds the target row.
- In `_advance_in`, where the fail-fast branch sets `BackfillStatus.FAILED`, and where the finalize branch sets `FAILED` or `SUCCESS`: record `BACKFILL_FAILED` or `BACKFILL_COMPLETED` with messages `f"Backfill of '{key}' failed"` / `f"Backfill of '{key}' completed"`.
- `events/types.py`'s docstring says the backfill events are recorded by the backfill store (not "the scheduler produces them"); `docs/guide/events.md` says when each is recorded.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db packages/interloper-scheduler -q -p no:cacheprovider > .superpowers/t6.log 2>&1; tail -2 .superpowers/t6.log`
Expected: PASS (hook tests unchanged: hooks still fire from the status poll).

- [ ] **Step 5: Full check, checkpoint (no commit)**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

---

### Task 7: The deferred fixes

**Files:**
- Modify: `packages/interloper-core/src/interloper/component/decorator.py`, the decorator modules (`asset`, `source`, `connection`, `config`, `destination`, `schema`, `interloper_sql/view/decorator.py`), `docs/reference/*.md` (decorator entries), `serializable/base.py`, `source/base.py`, `runner/run.py`, `runner/base.py`, `component/base.py`, `asset/base.py`, `asset/upstream.py`, `packages/interloper-sql/src/interloper_sql/view/base.py`, `partitioning/{base,list}.py`, `dag/base.py`
- Test: `tests/component/test_decorator.py`, `tests/source/test_base.py`, `tests/source/test_decorator.py`, `tests/runner/test_run.py`, `tests/runner/test_base.py`, `tests/runner/test_async_runner.py`, `tests/component/test_base.py`, `tests/asset/test_upstream.py`, `tests/partitioning/test_list.py`, `tests/partitioning/test_base.py`, `tests/dag/test_base.py`

**Interfaces:**
- Produces: `Decorator(anchor, *, doc: str = "", **settings)`; `Asset._upstream(self, name, target, window, metadata)` and `Upstream.read(cls, upstream, reader, window, metadata, *, parameter)`; `IbisView._upstream` follows.

- [ ] **Step 1: Write the failing tests**

```python
# tests/component/test_decorator.py
def test_a_decorator_carries_its_documentation():
    assert il.asset.__doc__.startswith("Create an Asset subclass from a decorated function.")
    assert il.source.__doc__.startswith("Create a Source subclass")
    assert il.asset(tags=["x"]).__doc__ == il.asset.__doc__


def test_the_refusal_does_not_offer_the_identity_field():
    with pytest.raises(TypeError) as error:
        il.Asset._sort_settings({"nope": 1})

    assert "'id'" not in str(error.value)


def test_the_hint_names_where_a_setting_lives():
    with pytest.raises(TypeError, match=r"'dialect' is a setting of View"):

        @il.asset(dialect="bigquery")
        def rows() -> list[dict[str, Any]]:
            return []
```

```python
# tests/source/test_base.py
def test_the_refusal_says_how_to_give_a_source_an_asset(self):
    other = FakeSourceWithAssets()

    with pytest.raises(ValidationError, match=r"assets=\{'fake_first': \{\.\.\.\}\}"):
        FakeSourceWithAssets(assets=[other.assets[0]])


def test_reconfiguring_the_assets_is_refused(self):
    source = FakeSourceWithAssets()

    with pytest.raises(TypeError, match=r"assets"):
        source(assets=[])
```

```python
# tests/source/test_decorator.py
def test_a_source_function_returning_something_else_is_refused():
    with pytest.raises(TypeError, match=r"returned 'str'"):

        @il.source
        def broken():
            return "not assets"
```

```python
# tests/runner/test_run.py (beside the module's First / ScriptedRunner fixtures)
class Other(il.Asset):
    """An asset independent of First."""

    def data(self) -> list[dict[str, Any]]:
        return []


async def test_a_fault_while_draining_still_cancels_and_fails_the_run(monkeypatch):
    def broken(self: RunState, operation: Operation, result: OperationResult) -> None:
        raise RuntimeError("recording failed")

    monkeypatch.setattr(RunState, "finish", broken)
    dag = il.DAG(
        First(id="first", destinations=[il.MemoryDestination()]),
        Other(id="other", destinations=[il.MemoryDestination()]),
    )

    result = await ScriptedRunner(max_workers=2).run(dag)

    assert result.status is ExecutionStatus.FAILED
    assert all(info.is_terminal for info in result.executions.values())


# tests/runner/test_base.py
async def test_reraise_is_recorded_on_the_run_span(span_exporter):
    @il.asset
    def broken() -> list[dict[str, Any]]:
        raise KeyError("missing")

    with pytest.raises(KeyError):
        await AsyncRunner(reraise=True).run(il.DAG(broken(id="broken", destinations=[il.MemoryDestination()])))

    run_span = next(span for span in span_exporter.get_finished_spans() if span.name == "interloper.runner.run")
    assert any(event.name == "exception" for event in run_span.events)


# tests/runner/test_async_runner.py
async def test_a_retried_attempt_starts_with_its_own_event_id():
    calls: list[int] = []

    @il.asset(retry=il.RetryPolicy(max_attempts=2, delay=0.0, jitter=0.0))
    def flaky() -> list[dict[str, Any]]:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("transient")
        return [{"x": 1}]

    events: list[Event] = []
    await AsyncRunner(on_event=events.append).run(
        il.DAG(flaky(id="flaky", destinations=[il.MemoryDestination()])), metadata={"run_id": "run-1"}
    )

    started = [event for event in events if event.type is il.EventType.OPERATION_STARTED]
    assert [event.metadata["attempt"] for event in started] == [1, 2]
    assert started[1].id == RunState.operation_event_id("run-1", "flaky", il.EventType.OPERATION_STARTED, attempt=2)
    assert started[0].id != started[1].id


async def test_a_fail_fast_run_repeats_no_event_id():
    @il.asset()
    def boom() -> list[dict[str, Any]]:
        raise ValueError("nope")

    @il.asset()
    def fine() -> list[dict[str, Any]]:
        return [{"x": 1}]

    events: list[Event] = []
    dag = il.DAG(
        boom(id="boom", destinations=[il.MemoryDestination()]),
        fine(id="fine-1", destinations=[il.MemoryDestination()]),
        fine(id="fine-2", destinations=[il.MemoryDestination()]),
    )
    await AsyncRunner(max_workers=1, fail_fast=True, on_event=events.append).run(dag)

    ids = [event.id for event in events]
    assert len(ids) == len(set(ids))
```

(`RunState` from `interloper.runner`; `Operation`, `OperationResult` from `interloper.operation` in `test_run.py`.)

```python
# tests/asset/test_upstream.py
async def test_the_missing_data_warning_names_the_parameter():
    il.MemoryDestination.clear()
    upstream = _Rows(id="orders", destinations=[il.MemoryDestination()])
    reader = _Rows(id="reader", destinations=[il.MemoryDestination()])
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        result = await Upstream.read(upstream, reader, None, {}, parameter="orders")
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)

    assert result.data is None
    assert any("for parameter 'orders'" in (event.metadata.get("message") or "") for event in captured)
```

(`_Rows` is the fixture Task 2 added to this file.)

```python
# tests/partitioning/test_list.py
def test_a_string_is_not_a_list_of_values():
    with pytest.raises(ValueError, match=r"a sequence of values"):
        il.ListPartitionConfig(column="region", values="eu")


def test_an_undeclared_value_reads_as_a_sentence_about_the_asset():
    config = il.ListPartitionConfig(column="region", values=("eu", "us"))

    with pytest.raises(PartitionError, match=r"^declares no partition 'apac'"):
        config.check(il.ListPartitionConfig(column="region", values=("apac",)).window("apac", "apac"))
```

```python
# tests/partitioning/test_base.py
def test_an_unknown_field_in_a_definition_is_a_config_error():
    with pytest.raises(ConfigError, match=r"bogus"):
        PartitionConfig.from_definition({"kind": "time", "column": "date", "bogus": 1})
```

```python
# tests/dag/test_base.py
def test_the_incompatible_partitioning_message_names_both_partitionings():
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily() -> list[dict[str, Any]]:
        return []

    @il.asset(partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.MONTH))
    def monthly(daily: il.Upstream) -> list[dict[str, Any]]:
        return []

    upstream = daily(id="daily", destinations=[il.MemoryDestination()])

    with pytest.raises(DAGError, match=r"'daily' \(partitioned by day\).*'monthly' \(partitioned by month\)"):
        il.DAG(upstream, monthly(id="monthly", destinations=[il.MemoryDestination()], daily=upstream))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests -q -p no:cacheprovider -k "documentation or identity_field or where_a_setting or how_to_give or reconfiguring_the_assets or something_else or while_draining or reraise_is_recorded or own_event_id or repeats_no_event or names_the_parameter or not_a_list or sentence_about or unknown_field or both_partitionings"`
Expected: FAIL.

- [ ] **Step 3: Decorator documentation**

`Decorator.__init__(self, anchor, *, doc: str = "", **settings)` stores `self.__doc__ = doc or type(self).__doc__` and a configured copy passes it on (`Decorator(self.anchor, doc=self.__doc__, **settings)`). Each decorator module moves its attribute docstring into `doc=`:

```python
asset = Decorator(
    Asset,
    doc="""Create an Asset subclass from a decorated function.

    ...the same text...
    """,
)
```

Then the docs reference renders it by inspection: each decorator entry in `docs/reference/*.md` becomes

```markdown
::: interloper.asset.decorator.asset
    options:
      force_inspection: true
```

Run: `UV_FROZEN=1 uv run zensical build --strict > .superpowers/docs.log 2>&1; echo $?; grep -c "Create an Asset subclass from a decorated function" site/reference/assets/index.html`
Expected: `0`, then at least `1`. If the docs build cannot render it (non-zero exit, or a count of `0`), record a ruling and use the fallback instead: a small subclass per kind carrying the docstring as its class docstring (`class AssetDecorator(Decorator[Asset]): """..."""`, `asset = AssetDecorator(Asset)`), the reference entries pointing at the instance as before.

- [ ] **Step 4: Refusals**

- `Serializable._refusal` lists the accepted names from `cls._settable()` (new classmethod: the public ClassVars and fields; `Component._settable()` leaves out `id`), and its hint reads `f"; '{name}' is a setting of {declaring.__name__}"`.
- `Source.model_post_init`'s foreign-asset message: `f"Asset '{asset.key}' already belongs to source '{asset.parent.qualified_key}'; give this source its overrides instead: assets={{'{asset.key}': {{...}}}}"`.
- `Source.body_from_function` raises `TypeError(f"Source function '{fn.__name__}' returned {type(returned).__name__!r}; return an asset class or a list of them")` unless it returned an `Asset` subclass or a list of them.
- `Component.__call__` refuses a change naming a field in `self._owned_by_field()` (`TypeError: f"{cls.__name__}.{name} is owned: build a new {cls.__name__} with its overrides instead"`).

- [ ] **Step 5: The walk and reraise**

`Run.walk`: the drain moves inside the fault handling:

```python
        self.start()
        inflight: dict[asyncio.Task[OperationResult], Operation] = {}
        try:
            await self._schedule(inflight)
        except Exception as fault:  # noqa: BLE001 - a fault of the run itself becomes the run's error
            self.fault = fault
        try:
            await self._drain(inflight)
        except Exception as fault:  # noqa: BLE001 - so is one while draining
            self.fault = self.fault or fault
        self.state.cancel_pending()
        return self.finish(format_exception(self.fault) if self.fault is not None else None)
```

and `_record` skips a canceled task (`if task.cancelled(): continue` before reading its exception). `Runner.run` calls `run.reraise()` inside the `with tracer().start_as_current_span(...)` block, after setting the span status (still inside the subscription block).

- [ ] **Step 6: The upstream warning and partitioning**

- `Asset._upstream(self, name, target, window, metadata)` passes `parameter=name` to `Upstream.read`; `_upstreams` calls `self._upstream(name, ...)`; `IbisView._upstream` takes and forwards `name`. `Upstream.read(..., *, parameter: str)` writes `f"Asset '{reader.key}' found no data in upstream '{upstream.qualified_key}' for parameter '{parameter}' at {window}; it is passed with data=None"`.
- `ListPartitionConfig.__post_init__` refuses a `str` for `values` first: `raise ValueError(f"List partitioning values are a sequence of values, not the string {self.values!r}.")`.
- `ListPartitionConfig._check`'s message: `f"declares no partition {partition.key!r} (declared: {list(self.values)})"`.
- `PartitionConfig.from_definition` wraps the construction: a `TypeError` from an unknown field becomes `ConfigError(f"Partitioning definition has fields its kind does not declare: {error}")`.
- `DAG._check_partition_dependencies`'s incompatible message names each end, through a `PartitionConfig.__str__` added on the base (`f"partitioned by {self.kind}"`, overridden by `TimePartitionConfig` as `f"partitioned by {self.granularity.value}"` and by `ListPartitionConfig` as `f"partitioned by list {list(self.values)}"`): `f"Invalid upstream: '{upstream.key}' ({upstream.partitioning}) and its dependent '{operation.key}' ({operation.partitioning}) accept different windows; a run has one window, so both ends of an edge must accept the same windows."`

- [ ] **Step 7: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages -q -p no:cacheprovider > .superpowers/t7.log 2>&1; tail -2 .superpowers/t7.log`
Expected: PASS.

- [ ] **Step 8: Full check, checkpoint (no commit)**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.

---

### Task 8: Internal names, stale text, examples, docs, live verification

**Files:**
- Modify: kernel modules named below; `docs/index.md`, `docs/guide/{execution,destinations,schema}.md`; `examples/runner/{async_,serial}.py`, `examples/assets/demo.py` and any other `asyncio.run` example; `plugins/interloper/skills/interloper-upgrade/SKILL.md`

- [ ] **Step 1: Internal names**

Rename (locals, private names, module constants), each within its module, with the house word:

| Where | From | To |
|---|---|---|
| runner/results.py | `k`, `v` | `operation_id`, `execution` |
| runner/base.py | `runner_cls` | `runner_class` |
| runner/state.py | `_OPERATION_EVENT_NS` | `_OPERATION_EVENT_NAMESPACE` |
| dag/base.py | `id`, `preds`, `pred_id`, `pred`, `span_attrs` | `operation_id`, `predecessors`, `predecessor_id`, `predecessor`, `span_attributes` |
| dag/base.py | `_resolve_declared` | `_bind_declared` |
| serializable/base.py | `_ENV_VAR_RE`, `k`/`v`, `sub`, `klass`/`klass_fields`, `ref=` | `_ENVIRONMENT_VARIABLE_PATTERN`, `key`/`value`, `substitute`, `base`/`base_fields`, `reference=` |
| component/base.py | `kids` | `children` |
| catalog/base.py | `defn`, `k`/`v` | `definition`, `key`/`definition` |
| events/event.py | `m`, `ts`, `type_val`, `timestamp_val`, `e`, `k`/`v`, `json_str` | `metadata`, `timestamp`, `type_value`, `timestamp_value`, `error`, `key`/`value`, `text` |
| events/console.py | `m` | `metadata` |
| events/bus.py | `inst`, `h` | `bus`, `handler` |
| events/step.py, utils/concurrency.py | `exc_type`, `exc`, `tb` | `exception_type`, `exception`, `traceback` |
| schema/base.py | `a`, `n`/`f`, `k`/`v`, `e`, `out`, `_resolve_field_type` | `argument`, `name`/`field`, `key`/`value`, `error`, `reconciled`, `_widen_types` |
| normalizer/base.py | `d`, `k`/`v`, `na_keys` | `row`, `key`/`value`, `null_keys` |
| representation/base.py | `lo`, `hi` | `lower`, `upper` |
| destination/{csv,file}.py | `f`, `k`/`v` | `file`, `key`/`value` |
| destination/formats.py | `k`/`v`, `fmt`, `sub`, `py_type`, `_py_type_to_arrow_type` | `key`/`value`, `file_format`, `child`, `python_type`, `_python_type_to_arrow_type` |
| destination/database.py | `d` | `destination` |
| destination/object_store.py | `k`/`v` | `key`/`value` |
| resource/fields.py | `prop`, `r`, `props_str`, `k`/`v` | `property_schema`, `name`, `properties_text`, `key`/`value` |
| partitioning/base.py, asset/base.py, source/base.py | `config_cls`, `source_cls`, `asset_cls` | `config_class`, `source_class`, `asset_class` |
| connection/base.py | `env_credential` | `environment_credential` |
| utils/concurrency.py | `coro`, `coros`, `c` | `coroutine`, `coroutines`, `coroutine` |
| utils/json.py, utils/time.py, utils/text.py | `k`/`v`, `o`, `e`, `_KEY_RE`, `_SNAKE_NON_ALNUM` | `key`/`value`, `value`, `error`, `_KEY_PATTERN`, `_SNAKE_NON_ALPHANUMERIC` |
| utils/stats.py | `q` | `percent` |

Public parameters keep their names except where this table says so for a private one. `normalize_columns_names` and `drop_na_columns` are persisted config fields and stay.

Section dividers use the house titles: `Helpers` becomes `Internals` (resource/fields.py, destination/object_store.py), `Constructors` becomes `Construction` (catalog/base.py), `Public static API` becomes `Public API` (events/bus.py), `Discovery internals` becomes `Internals` (catalog/base.py), `Identity & definition` stays in asset/base.py only if it matches the house list (`Identity` and `Definition` are; split into two dividers if both sections are long, else `Identity`).

- [ ] **Step 2: Stale text**

| Location | Fix |
|---|---|
| events/types.py:17 | `source_id` becomes `parent_id`; module docstring "Event type enumeration" |
| dag/base.py (the `_include_read_only_upstreams` docstring) | `_read_upstreams` becomes `Asset._upstreams` |
| dag/base.py:48 | drop the `Source ... _resolve` mention |
| asset/base.py (`classpath`, `definition`) | `AssetRef` becomes `AssetMeta`; the format is ``module:Source.Asset`` |
| component/base.py (`to_spec`) | `children` becomes `owned` |
| component/base.py (`_collect`) | drop "and again by a decorator that adds relations to a built class" |
| component/inherited.py (`install`) | a decorator redeclaration keeps the marker; the MRO walk stays for subclasses redeclaring the field |
| representation/base.py:188 | drop `.conformer` |
| schema/base.py:265 | `validate` becomes `validate_rows` |
| catalog/base.py:92 | `target` becomes `targets` |
| utils/time.py:100 | `*ts*` becomes `*value*` |
| docs/index.md:108, docs/guide/execution.md:98 | `RunResult(... partition=...)` becomes `window=` |
| docs/guide/destinations.md:34 | a wrong destination class raises `ConfigError` |
| docs/guide/schema.md:53 and the strategy test comment | one version for the `"auto"` alias (read `git log -S'"auto"' -- packages/interloper-core/src/interloper/normalizer` for the release that retired it) |
| plugins/interloper/skills/interloper-upgrade/SKILL.md:84 | the old `trickle_resources` row's "New" column says `trickle` was removed in core phase 4 |

Then `grep -rnw "AssetRef\|_read_upstreams\|source_id\|trickle_resources" packages/*/src docs --include='*.py' --include='*.md' | grep -v docs/superpowers` prints nothing outside the upgrade table.

- [ ] **Step 3: Examples**

Every `asyncio.run(...)` in `examples/` driving an interloper coroutine becomes `il.run(...)`, and the unused `asyncio` import goes.

Run each: `UV_FROZEN=1 uv run python examples/asset.py`, `examples/source.py`, `examples/catalog.py`, `examples/runner/async_.py`, `examples/runner/serial.py`, `examples/assets/demo.py`
Expected: each exits 0.

- [ ] **Step 4: Upgrade row**

Add above the phase 4 row of `plugins/interloper/skills/interloper-upgrade/SKILL.md`:

| unreleased (core phase 5) | `Partition.id`; `Upstream.records`, `Representation.to_records`/`from_records`, `interloper_pandas.dataframe_to_records`; `interloper.representation.View`, `Representation.of(data)`; `InProcessRun`, `ContainerRun`, `JobRun`; `from interloper.destination import PartitionFilter`; `SourceError`, `utils.to_slug_case`, `utils.require_import`; `Catalog.get(key, default)`, `period_range(..., reversed=)`, `import_from_path(path, target_type)`, `Schema.infer(..., name=)`; `InputField(title=...)` (any field factory); `source(assets=...)` | `Partition.key`; `Upstream.rows`, `to_rows`/`from_rows`, `dataframe_to_rows`; `TableData`, `TableData.from_data(data)`; `AsyncRun`, `DockerRun`, `KubernetesRun`; `from interloper.partitioning import PartitionFilter`; removed (nothing replaces them); `Catalog.get(key)` then `or default`, reverse the range yourself, check the type yourself, the inferred schema is `InferredSchema`; `label=`; build a new source with `assets={key: {...}}` |

- [ ] **Step 5: Full check, docs build, catalog**

Run: `UV_FROZEN=1 make check-python > .superpowers/check.log 2>&1; tail -3 .superpowers/check.log`
Expected: all green.
Run: `UV_FROZEN=1 uv run zensical build --strict > .superpowers/docs.log 2>&1; echo $?`
Expected: `0`.
Catalog: run the snapshot command on the base commit's checkout first (`git stash` is forbidden; use the phase 4 worktree at `.claude/worktrees/refactor/core-component`, which is the base) and on this tree, and compare:
`UV_FROZEN=1 uv run python -c "import json, interloper as il; print(json.dumps(il.Catalog.discover().dump(), sort_keys=True, default=str))"` in each; `diff -q` the two files.
Expected: identical.

- [ ] **Step 6: Live verification**

- The demo manifest through the CLI for one day (from `dev/`): `UV_FROZEN=1 uv run interloper run -f /Users/g/Dev/internal/interloper-manifests/manifests/demo.yaml --date 2026-01-01`; it completes 5/5.
- Backend on :3100 (the `DEV_ENV` variables and `interloper app --api --cron --worker --reaper`, no `--dev`; read `alembic_version` first and leave it as found). Mint a session; run the demo source for one day through the API (success, 5 operations each queued, started and completed); create a two-day backfill of the demo source through the API, wait for it to finish, and list the org's events: `backfill_started` then `backfill_completed` for its id, no `run_id`. Tear down, delete the session row.

- [ ] **Step 7: Checkpoint (no commit)**
