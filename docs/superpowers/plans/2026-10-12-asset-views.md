# Views and References Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two new kinds of asset, a view (its definition is materialized, as a warehouse view) and a reference (an existing table interloper reads but never writes), the BigQuery and DuckDB backends for them, the Facebook star schema and a GA4 export source built on them as raw SQL, and `sql_view`, a portable authoring layer on Ibis exercised on its own test model.

**Architecture:** `View` and `Reference` subclass `Asset` and keep kind `asset`, so lineage, relations, catalog and the read path are unchanged. A view's single destination answers two new `Destination` hooks, `reference(asset)` and `create_view(asset, definition)`; `DatabaseDestination` implements them for every SQL warehouse through four small backend hooks (`dialect`, `table_parts`, `replace_view`, `reaches`). The DAG's partition rule skips edges into assets that read no upstream data. `interloper-sql` adds `sql_view`, which hands `data()` Ibis tables built from each upstream's declared schema and compiles the returned expression per dialect.

**Tech Stack:** Python ≥3.10, pydantic v2, DuckDB (`interloper-duckdb`), BigQuery (`interloper-google-cloud`), Ibis 12 (`ibis-framework>=12,<13`, in `interloper-sql` only).

**Spec:** [docs/superpowers/specs/2026-10-12-asset-views-design.md](../specs/2026-10-12-asset-views-design.md)

## Global Constraints

- Run every Python command from the repo root with `uv run …`. Lint `uv run ruff check <paths>`, format `uv run ruff format <paths>`, type check `uv run ty check`, tests `uv run pytest <path> -q`.
- Python style rules in `.claude/rules/python-style.md` apply to every file: full names (`destination`, not `dest`), Google docstrings with every section that applies (`Args:`, `Returns:`, `Raises:`), no attribute-level comments, dash-padded section dividers only when a class is long enough to need them. Component docstrings (sources, assets, decorated functions) are product copy: summary plus prose, no mechanical sections.
- Say **upstream** for a bound `il.Upstream`, never "leg", in code, docstrings and tests.
- Test files mirror the source layout one to one (`tests/<pkg>/test_<module>.py`); fold tests into the module's existing file, never a standalone `test_<feature>.py`.
- The placeholder star schemas' **relation names and keys stay exactly as they are** (`ads_stats`, `campaigns` accepting `["facebook_ads.campaigns", "facebook_ads.campaigns_stats"]`, `ads`): production rows reference them, and a renamed relation would fail hydration of the Swarovski sources.
- Raw-SQL views written for the Facebook star schema use only the portable subset both BigQuery and DuckDB accept: `UNION ALL`, `CAST(x AS STRING)`, `COALESCE`, `ROW_NUMBER() OVER (...)`, `WHERE TRUE QUALIFY ...`, unquoted lowercase column names. No backticks, no `SAFE_CAST`, no `_TABLE_SUFFIX`.
- A raw-SQL view with `dialect=None` asserts its SQL is portable and is checked against no dialect; a view with `dialect="bigquery"` is refused on any other destination.
- `reads_upstream_data` lives on `Operation` (the DAG node), not only on `Asset`: the DAG check reads it without `getattr`. Assets inherit `True`; views and references set `False`.
- Commits: each task ends with one local commit on `feat/asset-views` (Conventional Commit subject as given, message ending with the line `By Digitl`). Never push, amend or rebase; the controller squashes before hand-off. Pre-commit runs ruff, ty and pytest on every commit; a task is not done until its commit passes them.
- No Postgres-server or warehouse integration tests in CI. DuckDB on a temporary file is the only real engine tests use; BigQuery is mocked with `MagicMock` as in `packages/interloper-google-cloud/tests/bigquery/test_destination.py`.

## Review Focus

1. A view whose upstream has **two destinations**, only one reachable: `reference` must pick the reachable one (preferring `default_destination_key`), not fail on the first. Pinned in Task 3 (`test_reference_prefers_the_default_destination_then_the_first_reachable`).
2. A view bound to **no upstream at all** on a many relation (a star schema with no accounts yet): the run must fail with a `ConfigError` that names the view, not a warehouse syntax error. Pinned in Task 8 (`test_a_fact_with_no_account_bound_fails_clearly`).
3. A **reference** inside a source whose dataset is derived from a field (GA4): the table reference must carry the derived dataset, not the source key. Pinned in Task 9 (`test_the_events_reference_lives_in_the_property_dataset`).
4. **A job's partition reaching a view**: a job passes its partition to every node, so a view must ignore it and produce the same definition for every partition, never a per-day variant. Pinned in Task 5 (`test_a_partitioned_run_recreates_the_same_view`).
5. A `sql_view` whose expression **drops or retypes a declared column**: must fail before any DDL with the column names in the message. Pinned in Task 11 (`test_an_expression_that_misses_a_declared_column_is_refused`).

---

### Task 1: `reads_upstream_data` and the DAG partition exemption

**Files:**
- Modify: `packages/interloper-core/src/interloper/operation/base.py` (around line 120, next to `partitioning`)
- Modify: `packages/interloper-core/src/interloper/dag/base.py:281-315` (`_check_partition_dependencies`)
- Test: `packages/interloper-core/tests/dag/test_base.py`

**Interfaces:**
- Produces: `Operation.reads_upstream_data: ClassVar[bool] = True`; `DAG._check_partition_dependencies` skips edges into operations whose `reads_upstream_data` is `False`.

- [ ] **Step 1: Write the failing test**

Add next to `FakePartitionedAsset` (line 45) in `packages/interloper-core/tests/dag/test_base.py`:

```python
class FakeDefinitionAsset(il.Asset):
    """Asset that reads no upstream data, the shape of a view."""

    reads_upstream_data: ClassVar[bool] = False
    upstream: il.Asset | None = il.Relation("asset", optional=True)
```

and in the class holding `test_non_partitioned_depending_on_partitioned_raises` (line 561):

```python
    def test_an_asset_reading_no_upstream_data_may_depend_on_a_partitioned_one(self):
        upstream = FakePartitionedAsset()
        downstream = FakeDefinitionAsset(upstream=upstream)
        dag = DAG(upstream, downstream)
        assert dag.get_predecessors(downstream.id) == [upstream.id]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest packages/interloper-core/tests/dag/test_base.py -q -k reading_no_upstream_data`
Expected: FAIL with `DAGError` ("cannot be an upstream of non-partitioned asset").

- [ ] **Step 3: Add the attribute and the exemption**

In `operation/base.py`, next to `partitioning: ClassVar[PartitionConfig | None] = None`:

```python
    reads_upstream_data: ClassVar[bool] = True
```

and extend the `Operation` class docstring with one paragraph:

```
    ``reads_upstream_data`` says whether executing the node reads its
    upstreams' data for the run's partition. An asset does; a view or a
    reference does not, which is what lets them sit, unpartitioned, on
    partitioned upstreams: the partition rule in the DAG only applies to
    nodes that read.
```

In `dag/base.py` `_check_partition_dependencies`, after `operation = self.operation_map[operation_id]`:

```python
            if not operation.reads_upstream_data:
                continue
```

and add to its docstring, after "Read-only upstreams are included on purpose; the read uses the dependent's partition against the upstream table.":

```
        A dependent that reads no upstream data (``reads_upstream_data`` is
        ``False``) is exempt: it refers to the whole upstream table rather than
        reading a partition of it.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/dag/test_base.py -q`
Expected: PASS, including `test_non_partitioned_depending_on_partitioned_raises` (unchanged).

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper/operation/base.py packages/interloper-core/src/interloper/dag/base.py packages/interloper-core/tests/dag/test_base.py
git commit -m "feat(core): exempt nodes that read no upstream data from the partition rule

By Digitl"
```

---

### Task 2: `Destination.reference` and `Destination.create_view`

**Files:**
- Modify: `packages/interloper-core/src/interloper/destination/base.py`
- Test: `packages/interloper-core/tests/destination/test_base.py`, `packages/interloper-core/tests/destination/test_memory.py`

**Interfaces:**
- Produces: `Destination.reference(self, asset: Asset) -> Any` and `Destination.create_view(self, asset: Asset, definition: Any) -> None`, both raising `ConfigError` by default.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-core/tests/destination/test_memory.py` (add `from interloper.errors import ConfigError` and `import pytest` if missing):

```python
class TestViews:
    """Memory holds no views: both hooks refuse."""

    def test_reference_is_refused(self):
        destination = il.MemoryDestination(id="memory")

        @il.asset
        def rows() -> list:  # noqa: D103
            return []

        with pytest.raises(ConfigError, match="cannot reference"):
            destination.reference(rows())

    def test_create_view_is_refused(self):
        destination = il.MemoryDestination(id="memory")

        @il.asset
        def rows() -> list:  # noqa: D103
            return []

        with pytest.raises(ConfigError, match="does not support views"):
            destination.create_view(rows(), "SELECT 1")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/destination/test_memory.py -q -k Views`
Expected: FAIL with `AttributeError: 'MemoryDestination' object has no attribute 'reference'`.

- [ ] **Step 3: Add the hooks**

In `destination/base.py`, add `from interloper.errors import ConfigError` and, under `TYPE_CHECKING`, `from interloper.asset.base import Asset` (add `TYPE_CHECKING` to the `typing` import). Then after `partition_row_counts`:

```python
    # -- Views -----------------------------------------------------------------

    def reference(self, asset: Asset) -> Any:
        """This destination's handle on an asset's data, for a view to build on.

        What a reference is depends on the destination: a SQL warehouse hands
        out a table reference, and a destination that holds no views hands
        out nothing. The default refuses.

        Args:
            asset: The asset a view wants to build on.

        Raises:
            ConfigError: Always; this destination holds no views.
        """
        raise ConfigError(f"Destination '{self.key}' cannot reference asset '{asset.qualified_key}': it holds no views")

    def create_view(self, asset: Asset, definition: Any) -> None:
        """Materialize a view's definition under the asset's table.

        Args:
            asset: The view being materialized; its ``table`` and ``dataset``
                name the view.
            definition: Whatever the view's ``data()`` returned, in this
                destination's own terms.

        Raises:
            ConfigError: Always; this destination holds no views.
        """
        raise ConfigError(f"Destination '{self.key}' does not support views")
```

Extend the `Destination` class docstring with: "Two more hooks, `reference` and `create_view`, serve views; a destination that holds no views leaves them refusing."

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/destination -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper/destination/base.py packages/interloper-core/tests/destination/test_memory.py
git commit -m "feat(core): give destinations the reference and create_view hooks

By Digitl"
```

---

### Task 3: `DatabaseDestination` implements the hooks for the SQL family

**Files:**
- Modify: `packages/interloper-core/src/interloper/destination/database.py`
- Modify: `packages/interloper-core/src/interloper/destination/__init__.py`
- Test: `packages/interloper-core/tests/destination/test_database.py`

**Interfaces:**
- Produces: `TableReference(name: str, parts: tuple[str, ...], schema: type[Schema] | None)` (frozen dataclass); `Compilable` protocol with `compile(self, dialect: str) -> str`; on `DatabaseDestination`: `dialect: ClassVar[str | None] = None`, `table_parts(table, dataset) -> tuple[str, ...]`, `qualified_name(table, dataset) -> str`, `replace_view(table, dataset, sql) -> None`, `reaches(other: Destination) -> bool`, `reference(asset) -> TableReference`, `create_view(asset, definition: str | Compilable) -> None`.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-core/tests/destination/test_database.py`, extend `RecordingDatabase` (line 15) with view support and add a second fake that has none:

```python
class RecordingDatabase(DatabaseDestination):
    """Database destination capturing every hook call."""

    dialect: ClassVar[str | None] = "duckdb"
    calls: ClassVar[list[tuple[str, Any]]] = []

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        object.__setattr__(self, "calls", [])  # noqa: PLC2801 - bypasses pydantic's __setattr__

    def insert(self, table, dataset, data, context):
        self.calls.append(("insert", (table, dataset, Representation.of(data).records)))

    def delete(self, table, dataset, where):
        self.calls.append(("delete", (table, dataset, where)))

    def select(self, table, dataset, where):
        self.calls.append(("select", (table, dataset, where)))
        return []

    def count(self, table, dataset, column):
        return {}

    def table_parts(self, table, dataset):
        return (dataset or "main", table)

    def replace_view(self, table, dataset, sql):
        self.calls.append(("replace_view", (table, dataset, sql)))


class ViewlessDatabase(RecordingDatabase):
    """Database destination that implements no view hook."""

    dialect: ClassVar[str | None] = None

    def table_parts(self, table, dataset):
        return DatabaseDestination.table_parts(self, table, dataset)

    def replace_view(self, table, dataset, sql):
        DatabaseDestination.replace_view(self, table, dataset, sql)
```

Add `from interloper.destination.database import TableReference` and `from interloper.errors import ConfigError` to the imports, then append:

```python
class TestReference:
    """A table reference names the asset's table where this destination can query it."""

    def test_reference_quotes_the_parts(self):
        destination = RecordingDatabase(id="db")
        asset = plain_asset(destinations=[destination])
        reference = destination.reference(asset)
        assert reference == TableReference(name='"main"."plain_asset"', parts=("main", "plain_asset"), schema=None)

    def test_reference_carries_the_declared_schema(self):
        class Rows(il.Schema):
            a: int

        @il.asset(schema=Rows)
        def typed_asset() -> list:  # noqa: D103
            return []

        destination = RecordingDatabase(id="db")
        reference = destination.reference(typed_asset(destinations=[destination]))
        assert reference.schema is Rows

    def test_reference_prefers_the_default_destination_then_the_first_reachable(self):
        here = RecordingDatabase(id="here")
        there = RecordingDatabase(id="there")
        asset = plain_asset(destinations=[there, here], default_destination_key=here.key)
        assert here.reference(asset).parts == ("main", "plain_asset")

        unreachable = il.MemoryDestination(id="memory")
        other = plain_asset(destinations=[unreachable, here])
        assert here.reference(other).parts == ("main", "plain_asset")

    def test_reference_refuses_an_asset_it_cannot_query(self):
        here = RecordingDatabase(id="here")
        elsewhere = RecordingDatabase(id="elsewhere")
        with pytest.raises(ConfigError, match="cannot query asset 'plain_asset'"):
            here.reference(plain_asset(destinations=[elsewhere]))

    def test_reference_refuses_an_asset_with_no_destination(self):
        with pytest.raises(ConfigError, match="no destination"):
            RecordingDatabase(id="db").reference(plain_asset())

    def test_a_backend_without_view_hooks_refuses(self):
        destination = ViewlessDatabase(id="db")
        with pytest.raises(ConfigError, match="does not support views"):
            destination.reference(plain_asset(destinations=[destination]))


class TestCreateView:
    """A definition is SQL text, or something that compiles to it for the dialect."""

    def test_sql_text_is_passed_through(self):
        destination = RecordingDatabase(id="db")
        destination.create_view(plain_asset(destinations=[destination]), "SELECT 1 AS a")
        assert destination.calls == [("replace_view", ("plain_asset", None, "SELECT 1 AS a"))]

    def test_a_compilable_is_compiled_for_the_dialect(self):
        class Definition:
            def compile(self, dialect: str) -> str:
                return f"SELECT '{dialect}' AS dialect"

        destination = RecordingDatabase(id="db")
        destination.create_view(plain_asset(destinations=[destination]), Definition())
        assert destination.calls == [("replace_view", ("plain_asset", None, "SELECT 'duckdb' AS dialect"))]

    def test_a_compilable_needs_a_dialect(self):
        class Definition:
            def compile(self, dialect: str) -> str:
                return "SELECT 1"

        destination = ViewlessDatabase(id="db")
        with pytest.raises(ConfigError, match="declares no dialect"):
            destination.create_view(plain_asset(destinations=[destination]), Definition())
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/destination/test_database.py -q -k "Reference or CreateView"`
Expected: FAIL with `ImportError: cannot import name 'TableReference'`.

- [ ] **Step 3: Implement**

In `destination/database.py`, add `from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable`, and under `TYPE_CHECKING`: `from interloper.asset.base import Asset` and `from interloper.schema import Schema`. After `PartitionFilter`:

```python
@dataclass(frozen=True)
class TableReference:
    """A table a SQL destination can query, as a view builds on it.

    Attributes:
        name: The qualified table name, quoted the way this destination's
            dialect wants it, ready to drop into SQL text.
        parts: The unquoted path to the table, outermost first (a BigQuery
            project, dataset and table; a DuckDB schema and table), for a
            builder that composes references itself.
        schema: The asset's declared schema, ``None`` when it declares none.
    """

    name: str
    parts: tuple[str, ...]
    schema: type[Schema] | None


@runtime_checkable
class Compilable(Protocol):
    """A view definition that renders itself for a dialect."""

    def compile(self, dialect: str) -> str:
        """Render the definition as SQL text for one dialect.

        Args:
            dialect: The destination's dialect name.

        Returns:
            The SQL text of the view's query.
        """
        ...
```

In `DatabaseDestination`, add `dialect: ClassVar[str | None] = None` as the first class attribute and extend the class docstring with one paragraph:

```
    Views are the SQL family's own: a `reference` is a
    `TableReference`, a definition is SQL text in this destination's
    ``dialect`` or a `Compilable` rendered for it, and `create_view`
    replaces the view under the asset's table. A backend supports views by
    naming its ``dialect`` and implementing `table_parts` and
    `replace_view`; `reaches` says which other destinations' tables a view
    here may query, the same instance by default.
```

Then, in a new `# -- Views ---` section after the backend hooks:

```python
    def table_parts(self, table: str, dataset: str | None) -> tuple[str, ...]:
        """The unquoted path to a table, outermost first.

        Args:
            table: Table name.
            dataset: The asset's dataset, or ``None`` for the destination's default.

        Returns:
            The path components, such as ``(project, dataset, table)``.

        Raises:
            ConfigError: Always; this backend does not support views.
        """
        raise ConfigError(f"Destination '{self.key}' does not support views")

    def qualified_name(self, table: str, dataset: str | None) -> str:
        """The qualified table name, quoted for this dialect.

        Double-quotes every part, the SQL standard; a backend whose dialect
        quotes differently overrides this.

        Args:
            table: Table name.
            dataset: The asset's dataset, or ``None`` for the destination's default.

        Returns:
            The quoted, dot-joined path.
        """
        return ".".join('"' + part.replace('"', '""') + '"' for part in self.table_parts(table, dataset))

    def replace_view(self, table: str, dataset: str | None, sql: str) -> None:
        """Create or replace a view from a query.

        Args:
            table: The view's name.
            dataset: The asset's dataset, or ``None`` for the destination's default.
            sql: The query, in this destination's dialect.

        Raises:
            ConfigError: Always; this backend does not support views.
        """
        raise ConfigError(f"Destination '{self.key}' does not support views")

    def reaches(self, other: Destination) -> bool:
        """Whether a view in this destination may query tables in *other*.

        Decided from configuration alone, never a network call: the same
        instance by default, the same warehouse and location where a backend
        knows better.

        Args:
            other: The destination holding an upstream's table.

        Returns:
            True when a query here can name tables there.
        """
        return other.id == self.id

    def reference(self, asset: Asset) -> TableReference:
        """The table an asset materializes to, as a view here may query it.

        The asset's ``default_destination_key`` destination is preferred,
        then the first one this destination `reaches`.

        Args:
            asset: The upstream asset.

        Returns:
            The table reference.

        Raises:
            ConfigError: If the asset has no destination, or none this
                destination reaches.
        """
        destinations: list[Destination] = asset.destinations
        if not destinations:
            raise ConfigError(f"Asset '{asset.qualified_key}' has no destination to reference")
        ordered = sorted(destinations, key=lambda candidate: candidate.key != asset.default_destination_key)
        for candidate in ordered:
            if isinstance(candidate, DatabaseDestination) and self.reaches(candidate):
                return TableReference(
                    name=candidate.qualified_name(asset.table, asset.dataset or None),
                    parts=candidate.table_parts(asset.table, asset.dataset or None),
                    schema=asset.schema,
                )
        raise ConfigError(
            f"Destination '{self.key}' cannot query asset '{asset.qualified_key}': "
            f"none of its destinations ({', '.join(d.key for d in destinations)}) is reachable"
        )

    def create_view(self, asset: Asset, definition: str | Compilable) -> None:
        """Replace the view under the asset's table with a definition.

        Args:
            asset: The view being materialized.
            definition: SQL text in this destination's dialect, or a
                `Compilable` rendered for it.

        Raises:
            ConfigError: If a `Compilable` is given and this destination
                declares no dialect.
        """
        if isinstance(definition, str):
            sql = definition
        elif self.dialect is None:
            raise ConfigError(f"Destination '{self.key}' declares no dialect to compile a view for")
        else:
            sql = definition.compile(self.dialect)
        self.replace_view(asset.table, asset.dataset or None, sql)
```

Export `TableReference` and `Compilable` from `destination/__init__.py` (add to the import line and `__all__`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/destination -q && uv run ruff check packages/interloper-core && uv run ty check`
Expected: PASS, no lint or type errors.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper/destination packages/interloper-core/tests/destination/test_database.py
git commit -m "feat(core): table references and view creation on database destinations

By Digitl"
```

---

### Task 4: `Reference`, the asset that already exists

**Files:**
- Create: `packages/interloper-core/src/interloper/asset/reference.py`
- Modify: `packages/interloper-core/src/interloper/asset/base.py` (`table` property, line 266; `Definition` class attributes)
- Modify: `packages/interloper-core/src/interloper/source/base.py:586` (`_resolve`)
- Modify: `packages/interloper-core/src/interloper/asset/decorator.py`
- Modify: `packages/interloper-core/src/interloper/asset/__init__.py`, `packages/interloper-core/src/interloper/__init__.py`
- Test: `packages/interloper-core/tests/asset/test_reference.py`, `packages/interloper-core/tests/asset/test_decorator.py`

**Interfaces:**
- Produces: `Asset.table_name: ClassVar[str | None] = None` (a given physical table name, formatted with the owning source's fields, instead of the composed one); `Reference(Asset)` with `reads_upstream_data = False`; `il.reference(table: str, *, key: str, **overrides) -> type[Reference]`.

- [ ] **Step 1: Write the failing tests**

Create `packages/interloper-core/tests/asset/test_reference.py`:

```python
"""Tests for references: assets interloper reads but never writes."""

from __future__ import annotations

import datetime as dt

import interloper as il
import pytest


class Events(il.Schema):
    event_date: str
    event_name: str


@il.source
class Export(il.Source):
    property_id: str = il.InputField(default="", discriminator=True)

    events = il.reference("events_*", key="events", schema=Events, tags=["Event"])


class TestReference:
    def test_a_reference_is_an_asset_that_reads_no_upstream_data(self):
        assert issubclass(il.Reference, il.Asset)
        assert il.Reference.reads_upstream_data is False
        assert il.Reference.kind == "asset"

    def test_the_table_is_the_given_name_formatted_with_the_source_fields(self):
        source = Export(property_id="123")
        assert source.events.table == "events_*"

        @il.source
        class Sharded(il.Source):
            property_id: str = il.InputField(default="", discriminator=True)
            events = il.reference("events_{property_id}", key="events")

        assert Sharded(property_id="123").events.table == "events_123"

    def test_the_dataset_is_the_sources(self):
        source = Export(property_id="123", dataset="analytics_123")
        assert source.events.dataset == "analytics_123"

    def test_the_decorator_routes_overrides(self):
        assert Export.events.schema is Events
        assert Export.events.tags == ["Event"]
        assert Export.events.key == "events"

    def test_materializing_does_nothing(self):
        memory = il.MemoryDestination()
        source = Export(property_id="123", destinations=[memory])
        il.DAG(source).materialize(il.TimePartition(dt.date(2026, 1, 1)))
        assert memory._storage == {}

    def test_a_reference_is_read_like_any_asset(self):
        memory = il.MemoryDestination()
        source = Export(property_id="123", destinations=[memory])
        context = il.IOContext(asset=source.events)
        memory.write(context, [{"event_date": "20260101", "event_name": "page_view"}])
        assert memory.read(context) == [{"event_date": "20260101", "event_name": "page_view"}]


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/asset/test_reference.py -q`
Expected: FAIL with `AttributeError: module 'interloper' has no attribute 'reference'`.

- [ ] **Step 3: Implement**

In `asset/base.py`, under `# Definition` add `table_name: ClassVar[str | None] = None`, and change the `table` property:

```python
    @property
    def table(self) -> str:
        """The physical table (or leaf) name this asset materializes to, or refers to.

        Composed by the owning source (see `asset_table`) and coerced to a
        valid identifier, unless ``table_name`` gives the name outright: then
        it is used as written, formatted with the owning source's fields, so
        a reference can name a table interloper never created, wildcard and
        all. Standalone assets use their class key.
        """
        if self.table_name is not None:
            source = self.source
            fields = {name: getattr(source, name) for name in type(source).model_fields} if source is not None else {}
            return self.table_name.format(**fields)
        source = self.source
        raw = source.asset_table(self) if source is not None else self.key
        return to_identifier(raw)
```

In `source/base.py` `_resolve`, replace `validate_key(asset.table)` with:

```python
            if asset.table_name is None:
                validate_key(asset.table)
```

Create `asset/reference.py`:

```python
"""Reference: an asset interloper reads but never writes, a table that already exists."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from interloper.asset.base import Asset

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow


class Reference(Asset):
    """An asset whose table exists before interloper does: nothing is created or written.

    A reference points at a table in a destination and knows how to read it.
    ``table_name`` names the table as written, formatted with the owning
    source's fields; the dataset is the asset's, the source's by default. A
    run does nothing. Downstream assets read it like any other asset, and
    views build on it like any other table.
    """

    reads_upstream_data: ClassVar[bool] = False

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Do nothing: the table exists already.

        Args:
            partition_or_window: Ignored.
            dag: Ignored.
            metadata: Ignored.

        Returns:
            ``None``.
        """
        return None
```

In `asset/decorator.py`, add after `asset()`:

```python
def reference(table: str, *, key: str, **overrides: Any) -> type[Reference]:
    """Declare a reference: an asset over a table that already exists.

    Written as a class attribute of a source, since a reference has no body:

    ```py
    @il.source
    class Export(il.Source):
        property_id: str = il.InputField(discriminator=True)

        events = il.reference("events_*", key="events", schema=Events)
    ```

    Args:
        table: The physical table name, used as written and formatted with
            the owning source's fields (``"events_{property_id}"``).
        key: The asset key, which names the attribute and the catalog entry.
        **overrides: Definition metadata and behaviour as for `asset`
            (``schema``, ``tags``, ``dataset``, ``partitioning``, ...).

    Returns:
        A Reference subclass.
    """
    return decorate(Reference, Reference, build=_build_reference_class, table_name=table, key=key, **overrides)


def _build_reference_class(
    target: type[Reference],
    *,
    classvars: dict[str, Any],
    fields: dict[str, Any],
    relations: dict[str, Relation],
) -> type[Reference]:
    """Build a Reference subclass from routed overrides.

    Args:
        target: The anchor class, `Reference` itself.
        classvars: Class-level attributes to set (``key``, ``table_name``, ``schema``, ...).
        fields: Pydantic field values to set, annotated from the anchor's fields.
        relations: Relations declared on the reference; a reference declares none.

    Returns:
        A dynamically created Reference subclass named after its key.
    """
    namespace: dict[str, Any] = {**classvars, **fields}
    if relations:
        namespace["relations"] = relations
    annotations = {name: target.model_fields[name].annotation for name in fields if name in target.model_fields}
    if annotations:
        namespace["__annotations__"] = annotations
    return type(classvars["key"], (target,), namespace)
```

with `from interloper.asset.reference import Reference` at the top. Export `Reference` and `reference` from `asset/__init__.py` and `interloper/__init__.py` (import line and `__all__`, alphabetical).

Note: `decorate` routes `table_name` and `key` as public ClassVars of the anchor, so they land in `classvars`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core/tests/asset packages/interloper-core/tests/source -q && uv run ruff check packages/interloper-core && uv run ty check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper packages/interloper-core/tests/asset/test_reference.py
git commit -m "feat(core): references, assets over tables interloper never writes

By Digitl"
```

---

### Task 5: `View`, the asset whose definition is materialized

**Files:**
- Create: `packages/interloper-core/src/interloper/asset/view.py`
- Modify: `packages/interloper-core/src/interloper/asset/decorator.py` (`build_asset_class(base=...)`, `view()`)
- Modify: `packages/interloper-core/src/interloper/asset/upstream.py` (docstring of `data`)
- Modify: `packages/interloper-core/src/interloper/asset/__init__.py`, `packages/interloper-core/src/interloper/__init__.py`
- Test: `packages/interloper-core/tests/asset/test_view.py`

**Interfaces:**
- Consumes: `Destination.reference` / `create_view` (Task 2), `TableReference` (Task 3), `reads_upstream_data` (Task 1).
- Produces: `View(Asset)` with `reads_upstream_data = False`, `dialect: ClassVar[str | None] = None`, `_definition(result) -> Any` hook (identity); `il.view(fn, *, relations=..., **overrides)` decorator; `build_asset_class(fn, *, base=Asset, classvars, fields, relations)` (public, replaces `_build_asset_class`).

- [ ] **Step 1: Write the failing tests**

Create `packages/interloper-core/tests/asset/test_view.py`:

```python
"""Tests for views: assets whose definition, not data, is materialized."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

import interloper as il
import pytest
from interloper.destination.database import DatabaseDestination, TableReference
from interloper.errors import ConfigError


class RecordingWarehouse(DatabaseDestination):
    """Warehouse double recording the views it is asked to create."""

    dialect: ClassVar[str | None] = "duckdb"
    views: ClassVar[list[tuple[str, str | None, str]]] = []

    def insert(self, table, dataset, data, context):
        pass

    def delete(self, table, dataset, where):
        pass

    def select(self, table, dataset, where):
        return []

    def count(self, table, dataset, column):
        return {}

    def table_parts(self, table, dataset):
        return (dataset or "main", table)

    def replace_view(self, table, dataset, sql):
        self.views.append((table, dataset, sql))


class BigQueryLike(RecordingWarehouse):
    """Warehouse double of another dialect."""

    dialect: ClassVar[str | None] = "bigquery"


@il.source
class Shop(il.Source):
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        return [{"date": context.partition_date, "total": 1.0}]


@il.source
class Reporting(il.Source):
    @il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def order_totals(self, orders: list[il.Upstream]) -> str:
        return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)

    @il.view
    def daily_totals(self, order_totals: il.Upstream) -> str:
        return f"SELECT date, SUM(total) AS total FROM {order_totals.data.name} GROUP BY date"


@pytest.fixture(autouse=True)
def _reset() -> None:
    RecordingWarehouse.views.clear()
    il.MemoryDestination.clear()


class TestDefinition:
    def test_a_view_is_an_asset_that_reads_no_upstream_data(self):
        assert issubclass(il.View, il.Asset)
        assert il.View.reads_upstream_data is False
        assert il.View.kind == "asset"
        assert Reporting.order_totals.dialect is None

    def test_upstream_relations_are_inferred_as_for_assets(self):
        relation = Reporting.daily_totals.relations["order_totals"]
        assert (relation.kind, relation.key, relation.many) == ("asset", "order_totals", False)


class TestRun:
    def test_a_run_hands_data_references_and_creates_the_view(self):
        warehouse = RecordingWarehouse(id="warehouse")
        shop_a = Shop(id="a", destinations=[warehouse], select=[])
        shop_b = Shop(id="b", destinations=[warehouse], select=[])
        reporting = Reporting(destinations=[warehouse])

        il.DAG(shop_a, shop_b, reporting).materialize()

        assert RecordingWarehouse.views == [
            (
                "order_totals",
                "reporting",
                'SELECT date, total FROM "shop"."orders" UNION ALL SELECT date, total FROM "shop"."orders"',
            ),
            ("daily_totals", "reporting", 'SELECT date, SUM(total) AS total FROM "reporting"."order_totals" GROUP BY date'),
        ]

    def test_upstream_data_is_the_destinations_reference(self):
        warehouse = RecordingWarehouse(id="warehouse")
        reporting = Reporting(destinations=[warehouse])
        seen: list[Any] = []

        @il.view(relations={"order_totals": il.Relation("asset", "reporting.order_totals")})
        def probe(order_totals: il.Upstream) -> str:  # noqa: D103
            seen.append(order_totals.data)
            return "SELECT 1"

        view = probe(destinations=[warehouse])
        view.bind("order_totals", reporting.order_totals)
        il.DAG(view).materialize()
        assert seen == [TableReference(name='"reporting"."order_totals"', parts=("reporting", "order_totals"), schema=None)]

    def test_a_partitioned_run_recreates_the_same_view(self):
        warehouse = RecordingWarehouse(id="warehouse")
        reporting = Reporting(destinations=[warehouse])
        shop = Shop(destinations=[warehouse], select=[])
        dag = il.DAG(shop, reporting)
        dag.materialize(il.TimePartition(dt.date(2026, 1, 1)))
        dag.materialize(il.TimePartition(dt.date(2026, 1, 2)))
        created = [sql for table, _, sql in RecordingWarehouse.views if table == "order_totals"]
        assert len(created) == 2 and created[0] == created[1]

    def test_a_failed_creation_fails_the_view(self):
        class Refusing(RecordingWarehouse):
            def replace_view(self, table, dataset, sql):
                raise RuntimeError("column date not found")

        warehouse = Refusing(id="warehouse")
        reporting = Reporting(destinations=[warehouse])
        shop = Shop(destinations=[warehouse], select=[])
        with pytest.raises(RuntimeError, match="column date not found"):
            il.DAG(shop, reporting).materialize()


class TestValidation:
    def test_an_upstream_in_an_unreachable_destination_is_a_config_error(self):
        here = RecordingWarehouse(id="here")
        elsewhere = RecordingWarehouse(id="elsewhere")
        shop = Shop(destinations=[elsewhere])
        reporting = Reporting(destinations=[here])
        with pytest.raises(ConfigError, match="shop.orders"):
            il.DAG(shop, reporting)

    def test_an_upstream_in_a_viewless_destination_is_a_config_error(self):
        warehouse = RecordingWarehouse(id="warehouse")
        shop = Shop(destinations=[il.MemoryDestination(id="memory")])
        reporting = Reporting(destinations=[warehouse])
        with pytest.raises(ConfigError, match="shop.orders"):
            il.DAG(shop, reporting)

    def test_a_view_needs_exactly_one_destination(self):
        shop = Shop(destinations=[RecordingWarehouse(id="a")])
        with pytest.raises(ConfigError, match="exactly one destination"):
            il.DAG(shop, Reporting(destinations=[RecordingWarehouse(id="a"), RecordingWarehouse(id="b")]))
        with pytest.raises(ConfigError, match="exactly one destination"):
            il.DAG(shop, Reporting())

    def test_a_raw_sql_view_must_match_its_destinations_dialect(self):
        @il.view(dialect="bigquery")
        def sharded() -> str:  # noqa: D103
            return "SELECT * FROM `p.d.events_*`"

        with pytest.raises(ConfigError, match="dialect 'bigquery'"):
            il.DAG(sharded(destinations=[RecordingWarehouse(id="duck")]))
        il.DAG(sharded(destinations=[BigQueryLike(id="bq")]))
```

`DAG.materialize` re-raises the first failure (`reraise=True` on the runner), which is why failing runs are asserted with `pytest.raises`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-core/tests/asset/test_view.py -q`
Expected: FAIL with `AttributeError: module 'interloper' has no attribute 'view'`.

- [ ] **Step 3: Implement**

Create `asset/view.py`:

```python
"""View: an asset whose definition is materialized, as a view in its destination."""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, Any, ClassVar, cast

from interloper.asset.base import Asset
from interloper.asset.upstream import Upstream
from interloper.errors import ConfigError, format_exception
from interloper.events import EventBus, EventType
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer
from interloper.utils.concurrency import invoke

if TYPE_CHECKING:
    from collections.abc import Mapping

    from interloper.component import Component
    from interloper.dag.base import DAG
    from interloper.destination import Destination
    from interloper.partitioning.base import Partition, PartitionWindow


class View(Asset):
    """An asset whose ``data()`` returns a definition, which its destination materializes as a view.

    A view reads no upstream data: each ``il.Upstream`` it receives carries
    the destination's reference to that upstream (a table reference on a SQL
    warehouse) in ``data``, and whatever ``data()`` returns goes to
    `Destination.create_view`. It has exactly one destination, and it is
    unpartitioned: a run recreates the view, cheaply and idempotently, so the
    view matches the wiring as of its last run.

    ``dialect`` pins a raw-SQL view to the warehouses whose dialect it is
    written in; ``None`` leaves the SQL unchecked, the author's assertion that
    it is portable.
    """

    reads_upstream_data: ClassVar[bool] = False
    dialect: ClassVar[str | None] = None

    # -- Execution -------------------------------------------------------------

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Build the definition and create the view.

        Args:
            partition_or_window: Must be ``None``: a view is unpartitioned.
            dag: DAG the upstream assets are looked up in.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The definition ``data()`` returned, or ``None`` if the view is disabled.
        """
        if not self.enabled:
            return None
        metadata = metadata or {}
        definition = self._definition(await self.run_async(partition_or_window, dag, metadata))
        destination = self._view_destination()
        event_metadata = self._event_metadata(metadata, partition_or_window)
        event_metadata["destination_key"] = destination.key
        EventBus.emit(EventType.DEST_WRITE_STARTED, metadata={**event_metadata, "message": f"Creating view '{self.key}'"})
        try:
            span_attributes = telemetry_attributes.from_metadata(event_metadata)
            with tracer().start_as_current_span("interloper.destination.write", attributes=span_attributes):
                await invoke(destination.create_view, self, definition)
            EventBus.emit(EventType.DEST_WRITE_COMPLETED, metadata={**event_metadata, "message": f"Created view '{self.key}'"})
        except Exception as error:
            EventBus.emit(
                EventType.DEST_WRITE_FAILED,
                metadata={
                    **event_metadata,
                    "error": format_exception(error),
                    "traceback": traceback.format_exc(),
                    "message": f"Failed to create view '{self.key}': {format_exception(error)}",
                },
            )
            raise
        return definition

    def validate_relations(self, nodes: Mapping[str, Component] | None = None) -> None:
        """Check the relations as any asset, then what only a view needs.

        One destination; a dialect, when declared, that the destination
        shares; every bound upstream reachable from that destination.

        Args:
            nodes: Every node materializing in the same run, keyed by id, or ``None``.

        Raises:
            ConfigError: If the view has no single destination, its dialect
                does not match the destination's, or an upstream cannot be
                referenced from it.
        """
        super().validate_relations(nodes)
        problems: list[str] = []
        destinations = self._destinations()
        if len(destinations) != 1:
            problems.append(f"a view needs exactly one destination, has {len(destinations)}")
        else:
            destination = destinations[0]
            declared = getattr(destination, "dialect", None)
            if self.dialect is not None and declared != self.dialect:
                problems.append(f"written in dialect '{self.dialect}' but destination '{destination.key}' speaks {declared!r}")
            for name in self.upstream_relations():
                for target in cast("list[Asset]", self._bound.get(name, [])):
                    try:
                        destination.reference(target)
                    except ConfigError as error:
                        problems.append(f"'{name}' upstream '{target.qualified_key}': {error}")
        if problems:
            raise ConfigError(f"View '{self.qualified_key}': " + "; ".join(problems))

    # -- Internals -------------------------------------------------------------

    def _definition(self, result: Any) -> Any:
        """What ``data()`` returned, as the destination receives it.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The result, untouched; a subclass may check or wrap it.
        """
        return result

    def _normalize_and_conform(self, result: Any) -> Any:
        """Leave a definition alone: it is not data.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The result, untouched.
        """
        return result

    def _view_destination(self) -> Destination:
        """The one destination this view is created in.

        Returns:
            The destination.

        Raises:
            ConfigError: If the view does not have exactly one destination.
        """
        destinations = self._destinations()
        if len(destinations) != 1:
            raise ConfigError(f"View '{self.qualified_key}' needs exactly one destination, has {len(destinations)}")
        return destinations[0]

    async def _read_upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Reference every upstream instead of reading it.

        Args:
            name: The ``data()`` parameter the upstreams are built for.
            targets: The bound upstream assets.
            dag: Unused; a reference needs no run.
            partition_or_window: Unused; a reference has no scope.
            metadata: Unused.

        Returns:
            One `Upstream` per target, its ``data`` the destination's reference.
        """
        destination = self._view_destination()
        return [Upstream(asset=target, data=destination.reference(target)) for target in targets]
```

Check that `upstream_relations()` exists on `Asset` (it is called in `dag/base.py:_upstream_targets`); use the method the DAG uses.

In `asset/decorator.py`: rename `_build_asset_class` to `build_asset_class`, give it `base: type[Asset] = Asset` as a keyword parameter, use `base` in `type(fn.__name__, (base,), namespace)` and `base.model_fields` for annotations, and update the docstring. Add:

```python
@overload
def view(fn: Callable[..., Any], /) -> type[View]: ...
@overload
def view(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[Callable[..., Any]], type[View]]: ...
def view(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[View] | Callable[..., type[View]]:
    """Create a View subclass from a decorated function.

    Like `asset`, but the function returns the view's definition instead of
    its rows: SQL text in the destination's dialect, or anything the
    destination compiles. Each upstream parameter receives the destination's
    reference to that upstream in ``data``:

    ```py
    @il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def order_totals(orders: list[il.Upstream]) -> str:
        return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)
    ```

    Args:
        fn: The function to turn into a view, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form.
        relations: As for `asset`.
        **overrides: As for `asset`, plus ``dialect`` to pin raw SQL to one
            warehouse dialect.

    Returns:
        A View subclass with the function as its ``data()`` method.
    """
    build = functools.partial(build_asset_class, base=View)
    if fn is not None:
        return decorate(View, fn, build=build, relations=relations, **overrides)

    def wrapper(fn: Callable[..., Any]) -> type[View]:
        return decorate(View, fn, build=build, relations=relations, **overrides)

    return wrapper
```

with `from interloper.asset.view import View`. In `asset/upstream.py`, extend the `data` attribute docstring: "For a view, the destination's reference to the upstream (a `TableReference` on a SQL warehouse) rather than data: a view reads nothing." Export `View` and `view` from `asset/__init__.py` and `interloper/__init__.py`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-core -q && uv run ruff check packages/interloper-core && uv run ty check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-core/src/interloper packages/interloper-core/tests/asset/test_view.py
git commit -m "feat(core): views, assets whose definition is materialized

By Digitl"
```

---

### Task 6: DuckDB backend

**Files:**
- Modify: `packages/interloper-duckdb/src/interloper_duckdb/destination.py`
- Test: `packages/interloper-duckdb/tests/test_destination.py`

**Interfaces:**
- Consumes: `DatabaseDestination.table_parts` / `replace_view` / `reaches` / `dialect` (Task 3).
- Produces: `DuckDBDestination.dialect == "duckdb"`; views created under the asset's schema and readable through `select`.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-duckdb/tests/test_destination.py` (imports: `from interloper.destination.database import TableReference`, `from interloper_duckdb import DuckDBConnection, DuckDBDestination`):

```python
class TestViews:
    """Views are created under the asset's schema and read like tables."""

    def test_reference_quotes_schema_and_table(self, destination):
        @il.asset(dataset="shop")
        def orders() -> list:  # noqa: D103
            return []

        assert destination.reference(orders(destinations=[destination])) == TableReference(
            name='"shop"."orders"', parts=("shop", "orders"), schema=None
        )

    def test_replace_view_creates_the_schema_and_the_view(self, destination):
        destination.connection.client.execute('CREATE SCHEMA shop; CREATE TABLE shop.orders AS SELECT 1 AS total')
        destination.replace_view("totals", "reporting", 'SELECT SUM(total) AS total FROM "shop"."orders"')
        destination.replace_view("totals", "reporting", 'SELECT SUM(total) * 2 AS total FROM "shop"."orders"')
        assert destination.select("totals", "reporting", None).to_dict("records") == [{"total": 2}]

    def test_reaches_the_same_database_only(self, destination, tmp_path):
        same = DuckDBDestination(id="other", connection=destination.connection)
        elsewhere = DuckDBDestination(
            id="elsewhere", connection=DuckDBConnection(id="x", database=str(tmp_path / "other.duckdb"))
        )
        assert destination.reaches(same)
        assert not destination.reaches(elsewhere)
        assert DuckDBDestination.dialect == "duckdb"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-duckdb/tests/test_destination.py -q -k Views`
Expected: FAIL with `ConfigError: ... does not support views`.

- [ ] **Step 3: Implement**

In `interloper_duckdb/destination.py` (add `ClassVar` to the typing import and `from interloper.destination import Destination`):

```python
    dialect: ClassVar[str | None] = "duckdb"
```

as the first class attribute of `DuckDBDestination`, and in a `# -- Views ---` section:

```python
    def table_parts(self, table: str, dataset: str | None) -> tuple[str, str]:
        """The schema and table a reference names.

        Args:
            table: Table name.
            dataset: The schema, or ``None`` for the destination's default.

        Returns:
            ``(schema, table)``.
        """
        return (self._schema(dataset), table)

    def replace_view(self, table: str, dataset: str | None, sql: str) -> None:
        """Create or replace a view, creating its schema first.

        Args:
            table: The view's name.
            dataset: The schema, or ``None`` for the destination's default.
            sql: The view's query.
        """
        with self._cursor() as cursor, _DDL_LOCK:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(self._schema(dataset))}")
            cursor.execute(f"CREATE OR REPLACE VIEW {self._ref(table, dataset)} AS {sql}")

    def reaches(self, other: Destination) -> bool:
        """Whether *other* is a DuckDB destination on the same database.

        Args:
            other: The destination holding an upstream's table.

        Returns:
            True for the same database file or MotherDuck database.
        """
        return isinstance(other, DuckDBDestination) and other.connection.database == self.connection.database
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-duckdb -q && uv run ruff check packages/interloper-duckdb`
Expected: PASS. `select` on a view works because `_columns` reads `information_schema.columns`, which lists views; if it does not on the installed DuckDB, extend `_columns` to union `duckdb_views()`.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-duckdb
git commit -m "feat(duckdb): views

By Digitl"
```

---

### Task 7: BigQuery backend

**Files:**
- Modify: `packages/interloper-google-cloud/src/interloper_google_cloud/bigquery/destination.py`
- Test: `packages/interloper-google-cloud/tests/bigquery/test_destination.py`

**Interfaces:**
- Produces: `BigQueryDestination.dialect == "bigquery"`; `qualified_name` returns the whole path in one pair of backticks (`` `project.dataset.table` ``, the form a wildcard table needs); `reaches` is BigQuery and the same location.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-google-cloud/tests/bigquery/test_destination.py` (import `TableReference` from `interloper.destination.database`):

```python
class TestViews:
    """Views are plain DDL through the client; references quote the whole path once."""

    def test_reference_backticks_the_whole_path(self):
        destination, _ = _make_destination(dataset="ds")

        @il.asset
        def orders() -> list:  # noqa: D103
            return []

        assert destination.reference(orders(destinations=[destination])) == TableReference(
            name="`test-proj.ds.orders`", parts=("test-proj", "ds", "orders"), schema=None
        )

    def test_replace_view_issues_create_or_replace(self):
        destination, mock_client = _make_destination(dataset="ds")
        destination.replace_view("totals", None, "SELECT 1 AS total")
        mock_client.query.assert_called_once_with("CREATE OR REPLACE VIEW `test-proj.ds.totals` AS SELECT 1 AS total")
        mock_client.query.return_value.result.assert_called_once()

    def test_reaches_bigquery_in_the_same_location(self):
        destination, _ = _make_destination(dataset="ds")
        same_location, _ = _make_destination(dataset="other", _project="other-proj")
        assert destination.reaches(same_location)
        other_location = BigQueryDestination(
            id="us", project="p", location="US", connection=GoogleCloudConnection(id="c", service_account_key=_SA_KEY)
        )
        assert not destination.reaches(other_location)
        assert BigQueryDestination.dialect == "bigquery"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-google-cloud/tests/bigquery/test_destination.py -q -k Views`
Expected: FAIL with `ConfigError: ... does not support views`.

- [ ] **Step 3: Implement**

In `bigquery/destination.py` (add `ClassVar` to typing imports; `from interloper.destination import Destination`):

```python
    dialect: ClassVar[str | None] = "bigquery"
```

and in a `# -- Views ---` section:

```python
    def table_parts(self, table: str, dataset: str | None) -> tuple[str, str, str]:
        """The project, dataset and table a reference names.

        Args:
            table: Table name.
            dataset: The BigQuery dataset, or ``None`` for the destination's default.

        Returns:
            ``(project, dataset, table)``.
        """
        return (self.project, self._resolve_dataset(dataset), table)

    def qualified_name(self, table: str, dataset: str | None) -> str:
        """The whole path in one pair of backticks, the form a wildcard table needs.

        Args:
            table: Table name.
            dataset: The BigQuery dataset, or ``None`` for the destination's default.

        Returns:
            ``` `project.dataset.table` ```.
        """
        return f"`{self._table_ref(table, dataset)}`"

    def replace_view(self, table: str, dataset: str | None, sql: str) -> None:
        """Create or replace a view, creating its dataset first.

        Args:
            table: The view's name.
            dataset: The BigQuery dataset, or ``None`` for the destination's default.
            sql: The view's query.
        """
        self._ensure_dataset(dataset)
        self.client.query(f"CREATE OR REPLACE VIEW {self.qualified_name(table, dataset)} AS {sql}").result()

    def reaches(self, other: Destination) -> bool:
        """Whether *other* is a BigQuery destination in the same location.

        BigQuery queries across projects but not across locations.

        Args:
            other: The destination holding an upstream's table.

        Returns:
            True for BigQuery in the same location.
        """
        return isinstance(other, BigQueryDestination) and other.location == self.location
```

If `_ensure_dataset` calls `get_dataset` on the mock and the test's mock raises nothing, the test passes; otherwise set `mock_client.get_dataset.return_value = MagicMock()` in the test.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-google-cloud -q && uv run ruff check packages/interloper-google-cloud && uv run ty check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-google-cloud
git commit -m "feat(bigquery): views

By Digitl"
```

---

### Task 8: The Facebook star schema as raw-SQL views

**Files:**
- Modify: `packages/interloper-assets/src/interloper_assets/facebook_ads_star_schema/source.py`
- Create: `packages/interloper-assets/src/interloper_assets/facebook_ads_star_schema/schemas/__init__.py`, `fact_ad_performance.py`, `dim_campaigns.py`, `dim_ads.py`, `dim_accounts.py`
- Test: `packages/interloper-assets/tests/facebook_ads_star_schema/test_source.py` (rewrite)

**Interfaces:**
- Consumes: `il.view`, `TableReference.name`, DuckDB views (Task 6).
- Produces: four views, unpartitioned, relation names and keys unchanged from the placeholder (`ads_stats`, `campaigns`, `ads`, plus the sibling `fact_ad_performance`).

- [ ] **Step 1: Write the failing tests**

Replace `packages/interloper-assets/tests/facebook_ads_star_schema/test_source.py`:

```python
"""Tests for the Facebook Ads star schema: views over every account's tables, run on DuckDB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import interloper as il
import pytest
from interloper.errors import ConfigError

from interloper_assets.facebook_ads.source import FacebookAds, FacebookAdsConnection
from interloper_assets.facebook_ads_star_schema.source import FacebookAdsStarSchema

duckdb = pytest.importorskip("interloper_duckdb")

DAY = dt.date(2026, 10, 1)


@pytest.fixture
def warehouse(tmp_path: Path):
    connection = duckdb.DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))
    return duckdb.DuckDBDestination(id="warehouse", connection=connection)


def _account(account_id: str, warehouse) -> FacebookAds:
    connection = FacebookAdsConnection(id="fb", access_token="x", app_id="1", app_secret="x")
    return FacebookAds(account_id=account_id, connection=connection, destinations=[warehouse], select=[])


def _seed(warehouse, account_id: str, *, campaigns_snapshot: bool) -> None:
    client = warehouse.connection.client
    client.execute("CREATE SCHEMA IF NOT EXISTS facebook_ads")
    client.execute(
        f"CREATE TABLE facebook_ads.ads_stats__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date_start, '{account_id}' AS account_id, 'Acct {account_id}' AS account_name, "
        "'EUR' AS account_currency, 'c1' AS campaign_id, 'Camp from stats' AS campaign_name, "
        "'s1' AS adset_id, 'Set' AS adset_name, 'a1' AS ad_id, 'Ad' AS ad_name, "
        "'facebook' AS publisher_platform, 'feed' AS platform_position, 'mobile' AS impression_device, "
        "100 AS impressions, 10 AS clicks, 80 AS reach, 12.5 AS spend"
    )
    client.execute(
        f"CREATE TABLE facebook_ads.campaigns_stats__{account_id} AS SELECT "
        f"DATE '2026-10-01' AS date_start, {account_id} AS account_id, 'c1' AS campaign_id, "
        "'Camp from report' AS campaign_name, 'OUTCOME_SALES' AS objective"
    )
    if campaigns_snapshot:
        client.execute(
            f"CREATE TABLE facebook_ads.campaigns__{account_id} AS SELECT "
            f"DATE '2026-10-01' AS date, 'c1' AS id, 'Camp from snapshot' AS name, '{account_id}' AS account_id, "
            "'OUTCOME_SALES' AS objective, 'ACTIVE' AS status"
        )


def _materialize(warehouse, *accounts: FacebookAds) -> FacebookAdsStarSchema:
    star = FacebookAdsStarSchema(destinations=[warehouse])
    il.DAG(*accounts, star).materialize()
    return star


def _rows(warehouse, table: str) -> list[dict]:
    frame = warehouse.select(table, "facebook_ads_star_schema", None)
    return frame.sort_values(list(frame.columns)).to_dict("records")


class TestWiring:
    def test_relations_are_the_placeholders(self):
        fact = FacebookAdsStarSchema.fact_ad_performance.relations["ads_stats"]
        assert (fact.key, fact.many, fact.optional) == ("facebook_ads.ads_stats", True, True)
        campaigns = FacebookAdsStarSchema.dim_campaigns.relations["campaigns"]
        assert campaigns.key == ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"]
        assert FacebookAdsStarSchema.dim_accounts.relations["fact_ad_performance"].key == "fact_ad_performance"

    def test_views_are_unpartitioned_and_tagged(self):
        for asset in FacebookAdsStarSchema.asset_types:
            assert issubclass(asset, il.View)
            assert asset.partitioning is None
            assert asset.schema is not None
        assert FacebookAdsStarSchema.fact_ad_performance.tags == ["Fact"]
        assert FacebookAdsStarSchema.dim_campaigns.tags == ["Dimension"]


class TestOnDuckDB:
    def test_the_fact_unions_every_account(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=True)
        _seed(warehouse, "222", campaigns_snapshot=False)
        _materialize(warehouse, first, second)
        rows = _rows(warehouse, "fact_ad_performance")
        assert [row["account_id"] for row in rows] == ["111", "222"]
        assert rows[0]["spend"] == 12.5 and rows[0]["date"] == DAY

    def test_a_campaign_takes_its_snapshot_over_its_report_over_the_facts(self, warehouse):
        first, second = _account("111", warehouse), _account("222", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=True)
        _seed(warehouse, "222", campaigns_snapshot=False)
        _materialize(warehouse, first, second)
        rows = {row["account_id"]: row for row in _rows(warehouse, "dim_campaigns")}
        assert rows["111"]["campaign_name"] == "Camp from snapshot"
        assert rows["111"]["status"] == "ACTIVE"
        assert rows["222"]["campaign_name"] == "Camp from report"
        assert rows["222"]["status"] is None

    def test_ads_and_accounts_come_from_the_facts(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=False)
        _materialize(warehouse, first)
        assert _rows(warehouse, "dim_ads") == [
            {"ad_id": "a1", "ad_name": "Ad", "adset_id": "s1", "adset_name": "Set", "campaign_id": "c1",
             "account_id": "111", "status": None, "date": DAY}
        ]
        assert _rows(warehouse, "dim_accounts") == [
            {"account_id": "111", "account_name": "Acct 111", "account_currency": "EUR", "date": DAY}
        ]

    def test_the_views_are_read_through_the_read_path(self, warehouse):
        first = _account("111", warehouse)
        _seed(warehouse, "111", campaigns_snapshot=False)
        star = _materialize(warehouse, first)
        frame = warehouse.read(il.IOContext(asset=star.dim_campaigns))
        assert frame["campaign_id"].tolist() == ["c1"]

    def test_a_missing_account_table_fails_the_view(self, warehouse):
        first = _account("111", warehouse)
        with pytest.raises(Exception, match="ads_stats__111"):
            il.DAG(first, FacebookAdsStarSchema(destinations=[warehouse])).materialize()

    def test_a_fact_with_no_account_bound_fails_clearly(self, warehouse):
        with pytest.raises(ConfigError, match="no upstream bound"):
            il.DAG(FacebookAdsStarSchema(destinations=[warehouse])).materialize()
```

`DAG.materialize` re-raises the first failure, so failing runs are asserted with `pytest.raises`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-assets/tests/facebook_ads_star_schema -q`
Expected: FAIL (`issubclass(asset, il.View)` false; DuckDB tests fail on empty results).

- [ ] **Step 3: Write the schemas**

`schemas/__init__.py`:

```python
"""Schemas of the Facebook Ads star schema, one class per view."""

from interloper_assets.facebook_ads_star_schema.schemas.dim_accounts import DimAccounts
from interloper_assets.facebook_ads_star_schema.schemas.dim_ads import DimAds
from interloper_assets.facebook_ads_star_schema.schemas.dim_campaigns import DimCampaigns
from interloper_assets.facebook_ads_star_schema.schemas.fact_ad_performance import FactAdPerformance

__all__ = ["DimAccounts", "DimAds", "DimCampaigns", "FactAdPerformance"]
```

`schemas/fact_ad_performance.py`:

```python
import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FactAdPerformance(Schema):
    """Ad performance of every Facebook Ads account, one row per ad, day, publisher platform, position and device."""

    date: dt.date | None = Field(default=None, description="The report day.")
    account_id: str | None = Field(default=None, description="Ad account id.")
    account_name: str | None = Field(default=None, description="Ad account name.")
    account_currency: str | None = Field(default=None, description="Currency of the account's amounts.")
    campaign_id: str | None = Field(default=None, description="Campaign id.")
    campaign_name: str | None = Field(default=None, description="Campaign name as reported with the metrics.")
    adset_id: str | None = Field(default=None, description="Ad set id.")
    adset_name: str | None = Field(default=None, description="Ad set name as reported with the metrics.")
    ad_id: str | None = Field(default=None, description="Ad id.")
    ad_name: str | None = Field(default=None, description="Ad name as reported with the metrics.")
    publisher_platform: str | None = Field(default=None, description="Platform the ad was shown on (facebook, instagram, ...).")
    platform_position: str | None = Field(default=None, description="Placement on the platform (feed, story, ...).")
    impression_device: str | None = Field(default=None, description="Device the ad was shown on.")
    impressions: int | None = Field(default=None, description="Times the ad was shown.")
    clicks: int | None = Field(default=None, description="Clicks on the ad.")
    reach: int | None = Field(default=None, description="People who saw the ad.")
    spend: float | None = Field(default=None, description="Amount spent, in the account currency.")
```

`schemas/dim_campaigns.py`:

```python
import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimCampaigns(Schema):
    """One row per Facebook Ads campaign, from its latest snapshot, else its latest report, else the facts."""

    campaign_id: str | None = Field(default=None, description="Campaign id, the key the facts join on.")
    campaign_name: str | None = Field(default=None, description="Campaign name.")
    account_id: str | None = Field(default=None, description="Ad account the campaign belongs to.")
    objective: str | None = Field(default=None, description="Campaign objective.")
    status: str | None = Field(default=None, description="Configured status, known from snapshots only.")
    date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
```

`schemas/dim_ads.py`:

```python
import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimAds(Schema):
    """One row per Facebook ad with its ad set, from its latest snapshot, else the facts."""

    ad_id: str | None = Field(default=None, description="Ad id, the key the facts join on.")
    ad_name: str | None = Field(default=None, description="Ad name.")
    adset_id: str | None = Field(default=None, description="Ad set the ad belongs to.")
    adset_name: str | None = Field(default=None, description="Ad set name, known from the facts.")
    campaign_id: str | None = Field(default=None, description="Campaign the ad belongs to.")
    account_id: str | None = Field(default=None, description="Ad account the ad belongs to.")
    status: str | None = Field(default=None, description="Configured status, known from snapshots only.")
    date: dt.date | None = Field(default=None, description="Day of the row this dimension was taken from.")
```

`schemas/dim_accounts.py`:

```python
import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class DimAccounts(Schema):
    """One row per Facebook Ads account, from the facts."""

    account_id: str | None = Field(default=None, description="Ad account id, the key the facts join on.")
    account_name: str | None = Field(default=None, description="Ad account name.")
    account_currency: str | None = Field(default=None, description="Currency of the account's amounts.")
    date: dt.date | None = Field(default=None, description="Latest report day the account appears in.")
```

- [ ] **Step 4: Write the source**

Replace `facebook_ads_star_schema/source.py`:

```python
"""Facebook Ads star schema: views over every Facebook Ads account's tables."""

from __future__ import annotations

import interloper as il
from interloper.errors import ConfigError

from interloper_assets.facebook_ads_star_schema import schemas


def _union(upstreams: list[il.Upstream], select_by_key: dict[str, str]) -> str:
    """One SELECT per upstream, by its asset key, joined by UNION ALL.

    Args:
        upstreams: The bound upstreams, each carrying a table reference.
        select_by_key: The column list to select from an upstream, keyed by its asset key.

    Returns:
        The UNION ALL of one SELECT per upstream.

    Raises:
        ConfigError: If no upstream is bound; a view over nothing has no columns.
    """
    if not upstreams:
        raise ConfigError("no upstream bound: a star schema view needs at least one account")
    return " UNION ALL ".join(
        f"SELECT {select_by_key[upstream.asset.key]} FROM {upstream.data.name}" for upstream in upstreams
    )


FACT_COLUMNS = (
    "date_start AS date, CAST(account_id AS STRING) AS account_id, account_name, account_currency, "
    "CAST(campaign_id AS STRING) AS campaign_id, campaign_name, CAST(adset_id AS STRING) AS adset_id, adset_name, "
    "CAST(ad_id AS STRING) AS ad_id, ad_name, publisher_platform, platform_position, impression_device, "
    "impressions, clicks, reach, spend"
)

CAMPAIGN_CANDIDATES = {
    "campaigns": (
        "CAST(id AS STRING) AS campaign_id, name AS campaign_name, CAST(account_id AS STRING) AS account_id, "
        "objective, status, date, 1 AS priority"
    ),
    "campaigns_stats": (
        "CAST(campaign_id AS STRING) AS campaign_id, campaign_name, CAST(account_id AS STRING) AS account_id, "
        "objective, CAST(NULL AS STRING) AS status, date_start AS date, 2 AS priority"
    ),
}

AD_CANDIDATES = {
    "ads": (
        "CAST(id AS STRING) AS ad_id, name AS ad_name, CAST(adset_id AS STRING) AS adset_id, "
        "CAST(NULL AS STRING) AS adset_name, CAST(campaign_id AS STRING) AS campaign_id, "
        "CAST(account_id AS STRING) AS account_id, status, date, 1 AS priority"
    ),
}


@il.source(tags=["Analytics"], icon="logos:facebook", maturity="alpha")
class FacebookAdsStarSchema(il.Source):
    """Star schema over every Facebook Ads account, as views in the warehouse.

    The fact unions each account's ad performance; the dimensions take each
    campaign and ad from its latest snapshot where one is collected, else
    from its latest report, else from the facts, so every key a fact carries
    resolves. Views are recreated on every run, so they follow the accounts
    bound to them.
    """

    @il.view(
        schema=schemas.FactAdPerformance,
        tags=["Fact"],
        relations={"ads_stats": il.Relation("asset", "facebook_ads.ads_stats", many=True, optional=True)},
    )
    def fact_ad_performance(self, ads_stats: list[il.Upstream]) -> str:
        """Ad performance of every account, one row per ad, day and placement breakdown."""
        return _union(ads_stats, {"ads_stats": FACT_COLUMNS})

    @il.view(
        schema=schemas.DimCampaigns,
        tags=["Dimension"],
        relations={
            "campaigns": il.Relation(
                "asset", ["facebook_ads.campaigns", "facebook_ads.campaigns_stats"], many=True, optional=True
            )
        },
    )
    def dim_campaigns(self, campaigns: list[il.Upstream], fact_ad_performance: il.Upstream) -> str:
        """One row per campaign, from its latest snapshot, else its latest report, else the facts."""
        candidates = [
            f"SELECT {CAMPAIGN_CANDIDATES[upstream.asset.key]} FROM {upstream.data.name}" for upstream in campaigns
        ]
        candidates.append(
            "SELECT campaign_id, campaign_name, account_id, CAST(NULL AS STRING) AS objective, "
            f"CAST(NULL AS STRING) AS status, date, 3 AS priority FROM {fact_ad_performance.data.name}"
        )
        return (
            "SELECT campaign_id, campaign_name, account_id, objective, status, date "
            f"FROM ({' UNION ALL '.join(candidates)}) WHERE TRUE "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY date DESC, priority) = 1"
        )

    @il.view(
        schema=schemas.DimAds,
        tags=["Dimension"],
        relations={"ads": il.Relation("asset", "facebook_ads.ads", many=True, optional=True)},
    )
    def dim_ads(self, ads: list[il.Upstream], fact_ad_performance: il.Upstream) -> str:
        """One row per ad with its ad set, from its latest snapshot, else the facts."""
        candidates = [f"SELECT {AD_CANDIDATES[upstream.asset.key]} FROM {upstream.data.name}" for upstream in ads]
        candidates.append(
            "SELECT ad_id, ad_name, adset_id, adset_name, campaign_id, account_id, CAST(NULL AS STRING) AS status, "
            f"date, 2 AS priority FROM {fact_ad_performance.data.name}"
        )
        return (
            "SELECT ad_id, ad_name, adset_id, adset_name, campaign_id, account_id, status, date "
            f"FROM ({' UNION ALL '.join(candidates)}) WHERE TRUE "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY date DESC, priority) = 1"
        )

    @il.view(schema=schemas.DimAccounts, tags=["Dimension"])
    def dim_accounts(self, fact_ad_performance: il.Upstream) -> str:
        """One row per ad account with its name and currency, from the facts."""
        return (
            "SELECT account_id, account_name, account_currency, date "
            f"FROM {fact_ad_performance.data.name} WHERE TRUE "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY date DESC) = 1"
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-assets -q && uv run ruff check packages/interloper-assets && uv run ty check`
Expected: PASS, including `test_entity_partitioning.py` (views are tagged Fact/Dimension, not Entity). If DuckDB rejects `WHERE TRUE QUALIFY`, drop `WHERE TRUE` for DuckDB only if BigQuery also accepts a bare `QUALIFY`; otherwise keep it, since both accept it.

- [ ] **Step 6: Commit**

```bash
git add packages/interloper-assets/src/interloper_assets/facebook_ads_star_schema packages/interloper-assets/tests/facebook_ads_star_schema
git commit -m "feat(assets): the Facebook Ads star schema as views

By Digitl"
```

---

### Task 9: The GA4 export source: a reference and a BigQuery-only view

**Files:**
- Create: `packages/interloper-assets/src/interloper_assets/google_analytics_4/__init__.py`, `source.py`, `schemas/__init__.py`, `schemas/events.py`, `schemas/fact_daily_events.py`
- Modify: `packages/interloper-assets/src/interloper_assets/__init__.py`, `dev/interloper.yaml`
- Test: `packages/interloper-assets/tests/google_analytics_4/test_source.py`

**Interfaces:**
- Consumes: `il.reference` (Task 4), `il.view(dialect=...)` (Task 5).
- Produces: `GoogleAnalytics4Export` source with `property_id`, a dataset derived as `analytics_{property_id}`, an `events` reference over `events_*` and a `fact_daily_events` view pinned to BigQuery.

- [ ] **Step 1: Write the failing tests**

Create `packages/interloper-assets/tests/google_analytics_4/test_source.py`:

```python
"""Tests for the GA4 export source: a reference to the export and a BigQuery-only view over it."""

from __future__ import annotations

from typing import ClassVar

import interloper as il
import pytest
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError

from interloper_assets.google_analytics_4.source import GoogleAnalytics4Export


class RecordingWarehouse(DatabaseDestination):
    dialect: ClassVar[str | None] = "bigquery"
    views: ClassVar[list[tuple[str, str | None, str]]] = []

    def insert(self, table, dataset, data, context):
        pass

    def delete(self, table, dataset, where):
        pass

    def select(self, table, dataset, where):
        return []

    def count(self, table, dataset, column):
        return {}

    def table_parts(self, table, dataset):
        return ("proj", dataset or "main", table)

    def qualified_name(self, table, dataset):
        return f"`{'.'.join(self.table_parts(table, dataset))}`"

    def replace_view(self, table, dataset, sql):
        self.views.append((table, dataset, sql))


class DuckLike(RecordingWarehouse):
    dialect: ClassVar[str | None] = "duckdb"


@pytest.fixture(autouse=True)
def _reset() -> None:
    RecordingWarehouse.views.clear()


def test_the_events_reference_lives_in_the_property_dataset():
    source = GoogleAnalytics4Export(property_id="123")
    assert source.events.table == "events_*"
    assert source.events.dataset == "analytics_123"
    assert isinstance(source.events, il.Reference)


def test_the_view_is_pinned_to_bigquery_and_reads_the_export():
    warehouse = RecordingWarehouse(id="bq")
    source = GoogleAnalytics4Export(property_id="123", destinations=[warehouse])
    il.DAG(source).materialize()
    [(table, dataset, sql)] = RecordingWarehouse.views
    assert (table, dataset) == ("fact_daily_events", "analytics_123")
    assert "`proj.analytics_123.events_*`" in sql
    assert "_TABLE_SUFFIX" in sql


def test_the_view_refuses_another_warehouse():
    with pytest.raises(ConfigError, match="dialect 'bigquery'"):
        il.DAG(GoogleAnalytics4Export(property_id="123", destinations=[DuckLike(id="duck")]))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-assets/tests/google_analytics_4 -q`
Expected: FAIL with `ModuleNotFoundError: interloper_assets.google_analytics_4`.

- [ ] **Step 3: Implement**

`google_analytics_4/__init__.py`: empty. `schemas/__init__.py`:

```python
"""Schemas of the GA4 export source."""

from interloper_assets.google_analytics_4.schemas.events import Events
from interloper_assets.google_analytics_4.schemas.fact_daily_events import FactDailyEvents

__all__ = ["Events", "FactDailyEvents"]
```

`schemas/events.py` (the export's top-level scalar columns; nested records are left to the view's SQL):

```python
from interloper.schema import Schema
from pydantic import Field


class Events(Schema):
    """The GA4 BigQuery export, one row per event, sharded daily into events_YYYYMMDD tables."""

    event_date: str | None = Field(default=None, description="The event's day as YYYYMMDD in the property's timezone.")
    event_timestamp: int | None = Field(default=None, description="When the event was logged, in microseconds since the epoch.")
    event_name: str | None = Field(default=None, description="The event's name (page_view, session_start, ...).")
    user_pseudo_id: str | None = Field(default=None, description="The pseudonymous user id.")
    platform: str | None = Field(default=None, description="The platform the event came from (WEB, ANDROID, IOS).")
    stream_id: str | None = Field(default=None, description="The data stream the event belongs to.")
```

`schemas/fact_daily_events.py`:

```python
import datetime as dt

from interloper.schema import Schema
from pydantic import Field


class FactDailyEvents(Schema):
    """Events and users per day and event name, from the GA4 export."""

    date: dt.date | None = Field(default=None, description="The event day.")
    event_name: str | None = Field(default=None, description="The event's name.")
    events: int | None = Field(default=None, description="Events logged that day.")
    users: int | None = Field(default=None, description="Distinct pseudonymous users that day.")
```

`source.py`:

```python
"""GA4 export: the BigQuery export of a Google Analytics 4 property, referenced where it lands."""

from __future__ import annotations

from typing import Any

import interloper as il

from interloper_assets.google_analytics_4 import schemas


@il.source(tags=["Analytics"], icon="logos:google-analytics", maturity="alpha")
class GoogleAnalytics4Export(il.Source):
    """The BigQuery export of a Google Analytics 4 property.

    Google writes the export itself, one ``events_YYYYMMDD`` table per day in
    the ``analytics_<property id>`` dataset of the project the property
    exports to. ``events`` references those tables; nothing is fetched or
    written. The daily fact is a view over them, BigQuery-only, since the
    sharded export exists nowhere else.
    """

    property_id: str = il.InputField(
        description="The GA4 property id; the export lands in the dataset analytics_<property id>",
        discriminator=True,
    )

    events = il.reference("events_*", key="events", schema=schemas.Events, tags=["Event"])

    @il.view(schema=schemas.FactDailyEvents, tags=["Fact"], dialect="bigquery")
    def fact_daily_events(self, events: il.Upstream) -> str:
        """Events and distinct users per day and event name, intraday tables left out."""
        return (
            "SELECT PARSE_DATE('%Y%m%d', event_date) AS date, event_name, "
            "COUNT(*) AS events, COUNT(DISTINCT user_pseudo_id) AS users "
            f"FROM {events.data.name} WHERE _TABLE_SUFFIX NOT LIKE 'intraday_%' "
            "GROUP BY 1, 2"
        )

    def model_post_init(self, context: Any) -> None:
        """Default the dataset to the property's export dataset before the assets inherit it.

        Args:
            context: Pydantic's post-init context, forwarded untouched.
        """
        if not self.dataset:
            self.dataset = f"analytics_{self.property_id}"
        super().model_post_init(context)
```

Register in `interloper_assets/__init__.py` (import `GoogleAnalytics4Export` from `interloper_assets.google_analytics_4.source`, alphabetical in both the imports and `__all__`) and add `  - interloper_assets.google_analytics_4.source.GoogleAnalytics4Export` to `dev/interloper.yaml` after the `google_ads` entry.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-assets -q && uv run ruff check packages/interloper-assets && uv run ty check`
Expected: PASS. If `test_entity_partitioning.py` complains about the `events` reference (tag `Event` is not `Entity`, so it should not), leave the tag as `Event`.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-assets dev/interloper.yaml
git commit -m "feat(assets): GA4 export source, a reference and a BigQuery view over it

By Digitl"
```

---

### Task 10: Ibis spike (throwaway)

**Files:**
- Create (scratchpad only, never committed): `<scratchpad>/ibis_spike.py`

**Interfaces:**
- Produces: a go/no-go for Task 11. If any criterion fails, stop and report; do not start Task 11.

- [ ] **Step 1: Write the spike**

```python
"""Throwaway: does Ibis 12 carry the star schema patterns on unbound tables, and run them on DuckDB?"""

import duckdb
import ibis

stats = ibis.table({"account_id": "int64", "campaign_id": "string", "campaign_name": "string",
                    "date_start": "date"}, name="campaigns_stats__111", database="facebook_ads")
snapshot = ibis.table({"id": "string", "name": "string", "account_id": "string", "date": "date"},
                      name="campaigns__111", database="facebook_ads")

candidates = ibis.union(
    snapshot.select(campaign_id=snapshot.id, campaign_name=snapshot.name,
                    account_id=snapshot.account_id, date=snapshot.date, priority=ibis.literal(1)),
    stats.select(campaign_id=stats.campaign_id, campaign_name=stats.campaign_name,
                 account_id=stats.account_id.try_cast("string"), date=stats.date_start, priority=ibis.literal(2)),
    distinct=False,
)
ranked = candidates.mutate(rank=ibis.row_number().over(group_by=candidates.campaign_id,
                                                        order_by=[candidates.date.desc(), candidates.priority]))
dimension = ranked.filter(ranked.rank == 0).drop("rank", "priority")

for dialect in ["bigquery", "duckdb", "snowflake", "clickhouse", "databricks", "mssql", "postgres"]:
    print(f"== {dialect}")
    print(ibis.to_sql(dimension, dialect=dialect))

connection = duckdb.connect()
connection.execute("CREATE SCHEMA facebook_ads")
connection.execute("CREATE TABLE facebook_ads.campaigns_stats__111 AS SELECT 111 AS account_id, 'c1' AS campaign_id, "
                   "'report' AS campaign_name, DATE '2026-10-01' AS date_start")
connection.execute("CREATE TABLE facebook_ads.campaigns__111 AS SELECT 'c1' AS id, 'snapshot' AS name, "
                   "'111' AS account_id, DATE '2026-10-01' AS date")
rows = connection.execute(ibis.to_sql(dimension, dialect="duckdb")).fetchall()
print("duckdb rows:", rows)
assert rows == [("c1", "snapshot", "111", __import__("datetime").date(2026, 10, 1))], rows
print("schema:", dict(dimension.schema().items()))
```

- [ ] **Step 2: Run it and record the footprint**

Run: `uv run --no-project --with "ibis-framework>=12,<13" --with duckdb --with packaging python <scratchpad>/ibis_spike.py`
Expected: SQL for all seven dialects, `duckdb rows: [('c1', 'snapshot', '111', datetime.date(2026, 10, 1))]`.

Then: `uv venv <scratchpad>/ibis-venv && uv pip install --python <scratchpad>/ibis-venv/bin/python "ibis-framework>=12,<13" && du -sh <scratchpad>/ibis-venv/lib/python*/site-packages && uv pip list --python <scratchpad>/ibis-venv/bin/python`
Record the size and the dependency list.

- [ ] **Step 3: Decide**

Go if: all seven dialects compile; DuckDB returns the snapshot row (latest-per-id with priority works); `try_cast` compiles on BigQuery to `SAFE_CAST`; the install without any backend is under 150 MB and pulls no compiled warehouse driver. Report the SQL for `mssql` and `bigquery` and the footprint in the task's summary. No commit: nothing in the repo changes.

---

### Task 11: `sql_view` in `interloper-sql`

**Files:**
- Modify: `packages/interloper-sql/pyproject.toml` (add `"ibis-framework>=12,<13"` to `dependencies`), then `uv lock`
- Create: `packages/interloper-sql/src/interloper_sql/view/__init__.py`, `base.py`, `decorator.py`, `types.py`
- Modify: `packages/interloper-sql/src/interloper_sql/__init__.py` (export `sql_view`, `SqlView`, `union`)
- Test: `packages/interloper-sql/tests/view/test_base.py`, `packages/interloper-sql/tests/view/test_types.py`, `packages/interloper-sql/tests/view/test_decorator.py`

**Interfaces:**
- Consumes: `View`, `View._definition`, `build_asset_class(base=...)` (Task 5), `TableReference` (Task 3).
- Produces: `interloper_sql.sql_view` decorator; `SqlView(View)`; `IbisDefinition(expression)` with `compile(dialect)`; `union(upstreams, **columns)`; `ibis_schema(schema: type[Schema]) -> ibis.Schema`; `IBIS_DIALECTS: dict[str, str]`.

- [ ] **Step 1: Write the failing tests**

`packages/interloper-sql/tests/view/__init__.py`: empty. `tests/view/test_types.py`:

```python
"""Tests for the schema and dialect mappings."""

from __future__ import annotations

import datetime as dt

import ibis
import interloper as il

from interloper_sql.view.types import IBIS_DIALECTS, ibis_schema


class Rows(il.Schema):
    id: str
    count: int | None
    amount: float | None
    flag: bool | None
    day: dt.date | None
    at: dt.datetime | None


def test_every_field_type_maps_to_an_ibis_type():
    assert ibis_schema(Rows) == ibis.schema(
        {"id": "string", "count": "int64", "amount": "float64", "flag": "boolean", "day": "date", "at": "timestamp"}
    )


def test_every_warehouse_dialect_has_an_ibis_name():
    assert IBIS_DIALECTS == {
        "bigquery": "bigquery",
        "duckdb": "duckdb",
        "snowflake": "snowflake",
        "clickhouse": "clickhouse",
        "databricks": "databricks",
        "tsql": "mssql",
        "postgres": "postgres",
    }
```

`tests/view/test_base.py`:

```python
"""Tests for SqlView: Ibis tables in, a compiled definition out."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

import ibis
import interloper as il
import pytest
from interloper.destination.database import DatabaseDestination
from interloper.errors import ConfigError

from interloper_sql import sql_view, union


class RecordingWarehouse(DatabaseDestination):
    dialect: ClassVar[str | None] = "duckdb"
    views: ClassVar[list[tuple[str, str | None, str]]] = []

    def insert(self, table, dataset, data, context):
        pass

    def delete(self, table, dataset, where):
        pass

    def select(self, table, dataset, where):
        return []

    def count(self, table, dataset, column):
        return {}

    def table_parts(self, table, dataset):
        return (dataset or "main", table)

    def replace_view(self, table, dataset, sql):
        self.views.append((table, dataset, sql))


class Orders(il.Schema):
    date: dt.date | None
    total: float | None


class Totals(il.Schema):
    date: dt.date | None
    total: float | None


@il.source
class Shop(il.Source):
    account_id: str = il.InputField(default="", discriminator=True)

    @il.asset(schema=Orders, partitioning=il.TimePartitionConfig(column="date"))
    def orders(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
        return []


@il.source
class Reporting(il.Source):
    @sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def totals(self, orders: list[il.Upstream]) -> ibis.Table:
        rows = union(orders)
        return rows.group_by(rows.date).aggregate(total=rows.total.sum())


@pytest.fixture(autouse=True)
def _reset() -> None:
    RecordingWarehouse.views.clear()


def _run(view_source: il.Source, *shops: il.Source) -> str:
    il.DAG(*shops, view_source).materialize()
    [(_, _, sql)] = RecordingWarehouse.views
    return sql


def test_upstreams_arrive_as_typed_ibis_tables_and_the_definition_compiles_for_the_dialect():
    warehouse = RecordingWarehouse(id="warehouse")
    sql = _run(Reporting(destinations=[warehouse]), Shop(account_id="1", destinations=[warehouse], select=[]))
    assert '"shop"."orders__1"' in sql
    assert "SUM(" in sql
    assert isinstance(Reporting.totals, type) and issubclass(Reporting.totals, il.View)


def test_union_stamps_a_column_per_upstream():
    class Stamped(il.Schema):
        date: dt.date | None
        total: float | None
        account_id: str | None

    @il.source
    class StampedReporting(il.Source):
        @sql_view(schema=Stamped, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders, account_id=lambda upstream: upstream.asset.source.discriminator)

    warehouse = RecordingWarehouse(id="warehouse")
    sql = _run(
        StampedReporting(destinations=[warehouse]),
        Shop(account_id="1", destinations=[warehouse], select=[]),
        Shop(account_id="2", destinations=[warehouse], select=[]),
    )
    assert "'1'" in sql and "'2'" in sql and "UNION ALL" in sql


def test_union_of_nothing_is_a_config_error():
    with pytest.raises(ConfigError, match="no upstream bound"):
        union([])


def test_an_expression_that_misses_a_declared_column_is_refused():
    @il.source
    class Wrong(il.Source):
        @sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
        def totals(self, orders: list[il.Upstream]) -> ibis.Table:
            return union(orders).select("date")

    warehouse = RecordingWarehouse(id="warehouse")
    shop = Shop(account_id="1", destinations=[warehouse], select=[])
    with pytest.raises(ConfigError, match=r"declares columns.*missing \['total'\]"):
        il.DAG(shop, Wrong(destinations=[warehouse])).materialize()
    assert RecordingWarehouse.views == []


def test_an_upstream_without_a_schema_is_refused():
    @il.source
    class Untyped(il.Source):
        @il.asset
        def rows(self) -> list[dict[str, Any]]:
            return []

    @il.source
    class Over(il.Source):
        @sql_view(schema=Totals, relations={"rows": il.Relation("asset", "untyped.rows")})
        def totals(self, rows: il.Upstream) -> ibis.Table:
            return rows.data

    warehouse = RecordingWarehouse(id="warehouse")
    untyped = Untyped(destinations=[warehouse], select=[])
    with pytest.raises(ConfigError, match="declares no schema"):
        il.DAG(untyped, Over(destinations=[warehouse])).materialize()
```

`tests/view/test_decorator.py`:

```python
"""Tests for the sql_view decorator."""

from __future__ import annotations

import ibis
import interloper as il

from interloper_sql import SqlView, sql_view


def test_sql_view_builds_a_sqlview_with_the_function_as_data():
    @sql_view(tags=["Fact"])
    def totals(orders: il.Upstream) -> ibis.Table:  # noqa: D103
        return orders.data

    assert issubclass(totals, SqlView)
    assert totals.key == "totals"
    assert totals.tags == ["Fact"]
    assert totals.relations["orders"].key == "orders"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv lock && uv sync --all-packages --all-extras && uv run pytest packages/interloper-sql/tests/view -q`
Expected: FAIL with `ImportError: cannot import name 'sql_view' from 'interloper_sql'`.

- [ ] **Step 3: Implement**

`view/types.py`:

```python
"""Ibis's view of interloper's types: schema fields and warehouse dialects, in one place."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import ibis
from interloper.schema import Schema

IBIS_TYPES: dict[Any, str] = {
    str: "string",
    int: "int64",
    float: "float64",
    bool: "boolean",
    dt.date: "date",
    dt.datetime: "timestamp",
    Decimal: "decimal",
    bytes: "binary",
}

IBIS_DIALECTS: dict[str, str] = {
    "bigquery": "bigquery",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "tsql": "mssql",
    "postgres": "postgres",
}


def ibis_schema(schema: type[Schema]) -> ibis.Schema:
    """The Ibis schema of a declared schema, every column nullable.

    A field whose type has no Ibis counterpart (a nested model, ``Any``) is
    typed ``string``: a view selects such a column through, never computes on it.

    Args:
        schema: The declared schema.

    Returns:
        The Ibis schema, in declaration order.
    """
    return ibis.schema({spec.name: IBIS_TYPES.get(spec.type, "string") for spec in schema.field_specs()})
```

`view/base.py`:

```python
"""SqlView: a view authored as an Ibis expression over its upstreams' declared schemas."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import ibis
from interloper.asset import Upstream, View
from interloper.destination.database import TableReference
from interloper.errors import ConfigError

from interloper_sql.view.types import IBIS_DIALECTS, ibis_schema

if TYPE_CHECKING:
    from interloper.asset import Asset
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow


class IbisDefinition:
    """A view definition that compiles an Ibis expression for a dialect."""

    def __init__(self, expression: ibis.Table) -> None:
        """Wrap an expression.

        Args:
            expression: The view's query as an Ibis table expression.
        """
        self.expression = expression

    def compile(self, dialect: str) -> str:
        """Render the expression as SQL for a warehouse dialect.

        Args:
            dialect: The destination's dialect name.

        Returns:
            The SQL text.

        Raises:
            ConfigError: If the dialect has no Ibis counterpart.
        """
        if dialect not in IBIS_DIALECTS:
            raise ConfigError(f"No Ibis dialect for '{dialect}'")
        return str(ibis.to_sql(self.expression, dialect=IBIS_DIALECTS[dialect]))


class SqlView(View):
    """A view whose ``data()`` receives Ibis tables and returns an Ibis expression.

    Each upstream's ``data`` is an ``ibis.Table`` named after the table the
    destination references and typed from the upstream's declared schema,
    so the expression is checked as it is built. The result must carry the
    view's declared columns with their declared types; the destination then
    compiles it for its own dialect.
    """

    async def _read_upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Turn every table reference into a typed Ibis table.

        Args:
            name: The ``data()`` parameter the upstreams are built for.
            targets: The bound upstream assets.
            dag: Forwarded to `View._read_upstreams`.
            partition_or_window: Forwarded.
            metadata: Forwarded.

        Returns:
            One `Upstream` per target, its ``data`` an ``ibis.Table``.

        Raises:
            ConfigError: If an upstream declares no schema, or its reference is not a table.
        """
        upstreams = await super()._read_upstreams(name, targets, dag, partition_or_window, metadata)
        tables: list[Upstream] = []
        for upstream in upstreams:
            reference = upstream.data
            if not isinstance(reference, TableReference):
                raise ConfigError(f"Upstream '{upstream.asset.qualified_key}' is not a table: {reference!r}")
            if reference.schema is None:
                raise ConfigError(f"Upstream '{upstream.asset.qualified_key}' declares no schema to type its table from")
            *outer, table = reference.parts
            database = outer[-1] if outer else None
            catalog = outer[-2] if len(outer) > 1 else None
            tables.append(
                Upstream(
                    asset=upstream.asset,
                    data=ibis.table(ibis_schema(reference.schema), name=table, database=database, catalog=catalog),
                )
            )
        return tables

    def _definition(self, result: Any) -> IbisDefinition:
        """Check the expression against the declared schema and wrap it.

        Args:
            result: The value ``data()`` returned.

        Returns:
            The compilable definition.

        Raises:
            ConfigError: If the result is not an Ibis table, the view
                declares no schema, or the expression's columns differ from
                the declared ones.
        """
        if not isinstance(result, ibis.Table):
            raise ConfigError(f"View '{self.qualified_key}' returned {type(result).__name__}, not an Ibis table")
        if self.schema is None:
            raise ConfigError(f"View '{self.qualified_key}' declares no schema")
        declared = {name: str(dtype) for name, dtype in ibis_schema(self.schema).items()}
        produced = {name: str(dtype) for name, dtype in result.schema().items()}
        if declared != produced:
            missing = sorted(set(declared) - set(produced))
            extra = sorted(set(produced) - set(declared))
            retyped = sorted(name for name in declared.keys() & produced.keys() if declared[name] != produced[name])
            raise ConfigError(
                f"View '{self.qualified_key}' declares columns its expression does not produce: "
                f"missing {missing}, extra {extra}, retyped {retyped}"
            )
        return IbisDefinition(result)


def union(upstreams: Sequence[Upstream], **columns: Callable[[Upstream], Any]) -> ibis.Table:
    """The UNION ALL of every upstream's table, optionally stamping a column per upstream.

    Args:
        upstreams: The bound upstreams of a many-valued relation, each carrying an ``ibis.Table``.
        **columns: Column name to a function of the upstream giving that
            upstream's constant value, such as the account it came from.

    Returns:
        One table over every upstream.

    Raises:
        ConfigError: If no upstream is bound; a view over nothing has no columns.
    """
    if not upstreams:
        raise ConfigError("no upstream bound: a view needs at least one")
    tables = [
        upstream.data.mutate(**{name: ibis.literal(value(upstream)) for name, value in columns.items()})
        for upstream in upstreams
    ]
    return ibis.union(*tables, distinct=False)
```

`view/decorator.py`:

```python
"""Decorator for creating SqlView subclasses from functions."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, overload

from interloper.asset.decorator import build_asset_class
from interloper.component.decorator import decorate

from interloper_sql.view.base import SqlView


@overload
def sql_view(fn: Callable[..., Any], /) -> type[SqlView]: ...
@overload
def sql_view(
    *,
    relations: dict[str, Any] = ...,
    **overrides: Any,
) -> Callable[[Callable[..., Any]], type[SqlView]]: ...
def sql_view(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    relations: dict[str, Any] | None = None,
    **overrides: Any,
) -> type[SqlView] | Callable[..., type[SqlView]]:
    """Create a SqlView subclass from a decorated function.

    Like ``il.view``, but portable: each upstream parameter receives a typed
    ``ibis.Table`` in ``data``, the function returns an Ibis expression, and
    the destination compiles it for its own dialect:

    ```py
    @sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def totals(orders: list[il.Upstream]) -> ibis.Table:
        rows = union(orders)
        return rows.group_by(rows.date).aggregate(total=rows.total.sum())
    ```

    Args:
        fn: The function to turn into a view, passed positionally when the
            decorator is used bare. ``None`` in the parenthesised form.
        relations: As for ``il.asset``.
        **overrides: As for ``il.asset``; a schema is required.

    Returns:
        A SqlView subclass with the function as its ``data()`` method.
    """
    build = functools.partial(build_asset_class, base=SqlView)
    if fn is not None:
        return decorate(SqlView, fn, build=build, relations=relations, **overrides)

    def wrapper(fn: Callable[..., Any]) -> type[SqlView]:
        return decorate(SqlView, fn, build=build, relations=relations, **overrides)

    return wrapper
```

`view/__init__.py`:

```python
"""Portable views: Ibis expressions over typed upstream tables, compiled per warehouse dialect."""

from interloper_sql.view.base import IbisDefinition, SqlView, union
from interloper_sql.view.decorator import sql_view
from interloper_sql.view.types import IBIS_DIALECTS, ibis_schema

__all__ = ["IBIS_DIALECTS", "IbisDefinition", "SqlView", "ibis_schema", "sql_view", "union"]
```

Re-export `SqlView`, `sql_view` and `union` from `interloper_sql/__init__.py`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-sql -q && uv run ruff check packages/interloper-sql && uv run ty check`
Expected: PASS. If `ibis.table(..., catalog=None)` rejects `None`, pass the keyword only when set.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-sql uv.lock
git commit -m "feat(sql): sql_view, portable views authored as Ibis expressions

By Digitl"
```

---

### Task 12: Documentation

**Files:**
- Create: `docs/guide/views.md`
- Modify: `zensical.toml` (nav, after `{ "Dependencies" = "guide/dependencies.md" }`), `docs/guide/dependencies.md` (one paragraph), `AGENTS.md` (asset naming: the Fact and Dimension tags)

- [ ] **Step 1: Write the guide**

`docs/guide/views.md`:

````markdown
# Views & references

Two kinds of asset besides the one that writes rows. A **view** materializes its definition: its
destination creates a warehouse view from what `data()` returns. A **reference** materializes
nothing: it points at a table that already exists and reads it. Both are assets in every other
respect, with lineage, relations, a schema and a place in the catalog.

| | Asset | View | Reference |
|---|---|---|---|
| `data()` returns | rows | a view definition | nothing to return |
| A run | conforms and writes | creates or replaces the view | does nothing |
| `upstream.data` | the data read | the destination's reference to the upstream | |

## Views

```py
@il.source
class Reporting(il.Source):
    @il.view(relations={"orders": il.Relation("asset", "shop.orders", many=True)})
    def order_totals(self, orders: list[il.Upstream]) -> str:
        return " UNION ALL ".join(f"SELECT date, total FROM {upstream.data.name}" for upstream in orders)
```

A view has exactly one destination, declared like any asset's. Each upstream parameter receives
the destination's reference to that upstream in `data`: on a SQL warehouse a `TableReference`,
whose `name` is the qualified table, quoted for the dialect. The function composes the definition
from those references; a many-valued relation is a list, and the author writes the `UNION ALL`.

A raw-SQL view is written in one dialect. `dialect="bigquery"` pins it to BigQuery destinations;
the default, `None`, is the author's assertion that the SQL is portable, and it is checked against
no dialect.

A view is unpartitioned and reads no upstream data, so it may sit on partitioned upstreams: it
refers to the whole table, and the warehouse resolves the query. It is recreated on every run,
cheaply and idempotently, so it follows the accounts bound to it. Put it in a job after its
connectors.

What the graph checks, when it is built: the view has one destination; its dialect, when declared,
is the destination's; every bound upstream is reachable from that destination (`reaches`, decided
from configuration: same BigQuery location, same DuckDB database). Anything the query gets wrong is
the warehouse's verdict when the view is created, reported as the view's failure.

### Portable views

`interloper-sql` adds `sql_view`: each upstream's `data` is an `ibis.Table` typed from the
upstream's declared schema, the function returns an Ibis expression, and the destination compiles it
for its own dialect. The expression's columns must match the view's declared schema.

```py
from interloper_sql import sql_view, union

@sql_view(schema=Totals, relations={"orders": il.Relation("asset", "shop.orders", many=True)})
def totals(self, orders: list[il.Upstream]) -> ibis.Table:
    rows = union(orders, account_id=lambda upstream: upstream.asset.source.discriminator)
    return rows.group_by([rows.date, rows.account_id]).aggregate(total=rows.total.sum())
```

Every upstream of a `sql_view` must declare a schema: that is what types its table.

## References

```py
@il.source
class GoogleAnalytics4Export(il.Source):
    property_id: str = il.InputField(discriminator=True)

    events = il.reference("events_*", key="events", schema=Events)
```

A reference names its table as written, formatted with the owning source's fields
(`"events_{property_id}"`); the dataset is the asset's, the source's by default. Nothing is created
or written. Downstream assets read it like any asset, and views build on it like any table.

## Backends

| Destination | Views |
|---|---|
| BigQuery | yes; `reaches` the same location |
| DuckDB | yes; `reaches` the same database |
| Snowflake, ClickHouse, Databricks, Fabric, SQL | not yet: `table_parts`, `replace_view` and `reaches` to implement |
| GCS, S3, CSV, files, Sheets, Memory | no: a view there is a configuration error |
````

- [ ] **Step 2: Link it**

In `zensical.toml`, add `{ "Views & references" = "guide/views.md" },` after the Dependencies entry. In `docs/guide/dependencies.md`, after the "Reading upstream data" table, add: "A view's upstreams are not read at all: `data` holds the destination's reference to the upstream instead. See [Views & references](views.md)." In `AGENTS.md`, under "Asset naming", add a fourth bullet: "**Star schema** — views over the categories above, tagged `Fact` (`fact_<process>`, e.g. `fact_ad_performance`) or `Dimension` (`dim_<noun>`, e.g. `dim_campaigns`), unpartitioned."

- [ ] **Step 3: Build the docs**

Run: `uv run zensical build 2>&1 | tail -3`
Expected: builds without a warning about `views.md`.

- [ ] **Step 4: Commit**

```bash
git add docs/guide/views.md docs/guide/dependencies.md zensical.toml AGENTS.md
git commit -m "docs: views and references

By Digitl"
```

---

## Self-review notes

- Spec coverage: decisions 1 (Tasks 4, 5), 2 (Tasks 2, 5), 3 (Tasks 3, 6, 7), 4 (Task 5 validation, Task 9 dialect test), 5 (Task 1), 6 (Tasks 4, 9), 7 (Tasks 10 and 11), 8 (every task). The spec's "declared schema that does not match the table fails validation" for references is **not implemented**: no run touches a reference, so there is no moment to check it offline; a mismatch surfaces when a consumer reads or a view is created. Flagged for the spec review.
- The spec's `Asset.reads_upstream_data` is placed on `Operation` so the DAG reads it without `getattr`; assets inherit it.
- The spec's reference example `dataset="analytics_{property_id}"` is realised by the GA4 source deriving its dataset in `model_post_init`; `table_name` is the only templated attribute. Flagged for the spec review.
- Type consistency: `TableReference(name, parts, schema)` in Tasks 3, 5, 6, 7, 11; `table_parts` / `qualified_name` / `replace_view` / `reaches` / `dialect` in Tasks 3, 6, 7, 9, 11; `build_asset_class(base=)` in Tasks 5 and 11; `_definition` in Tasks 5 and 11.
- The Facebook star schema stays raw SQL (Task 8); `sql_view` is exercised on its own test model (Task 11). Porting Facebook would make `interloper-assets` depend on `interloper-sql` and its drivers, deferred per the spec.
