# Overview Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `/` redirect with the Overview page designed in `UI - Overview.dc.html`: health strip, needs-attention list, timeline with scheduled ghosts, partition coverage calendar, coming up / just happened, and a components inventory.

**Architecture:** `interloper-db` gains three aggregate reads (`runs.latest_by_target`, `events.latest_by_component`, `events.coverage_rows`). `interloper-api` gains `routes/overview.py` with `GET /overview` and `GET /overview/coverage`, which own the page's semantics as pure functions over store rows. The Nuxt app gains an `overview` store, a `types/overview.ts`, seven components under `components/overview/`, and small extensions to `ChartExecutionTimeline`, `ExecutionsRunModal` and the components `[kind]` page.

**Tech Stack:** Python 3.10+ (SQLModel, FastAPI, pydantic), pytest over in-memory SQLite; Nuxt 4 + Nuxt UI 4 + Pinia, ECharts 6 via vue-echarts (already a dependency), Tailwind 4.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-09-30-overview-page-design.md`. Read it first; its Definitions section is the vocabulary every task uses.
- **No new dependencies** anywhere (no `croniter` in the API, no toolkit dependency in the API, no new npm packages).
- Python: ruff line length 120, type-checked with `ty`; run `uv run ruff check`, `uv run ty check`, `uv run pytest packages/<pkg>` from the repo root. Full Google-style docstrings (Args/Returns/Raises) on every function, private ones included.
- Test files mirror the package layout: store tests fold into `packages/interloper-db/tests/store/test_runs.py` and `test_events.py`; API tests live in `packages/interloper-api/tests/routes/test_overview.py`.
- Frontend: 4-space indentation, `pnpm run lint` and `pnpm exec nuxt typecheck` from `packages/interloper-app/app/`. Components under `components/overview/` auto-import as `Overview<Name>`; `components/ui/` has no prefix.
- Never write the em dash character (U+2014) in code, comments, docs or copy. Use ":" or "," or a plain hyphen.
- Comment sparingly: only a non-obvious *why*, scoped to the code it sits on.
- Colours: canvas code reads `CHART_STATUS_COLORS` / `CHART_AXIS_COLORS` from `app/utils/chartColors.ts`; DOM code uses Nuxt UI semantic classes (`text-error`, `bg-success/10`, `text-muted`, `border-default`, `bg-muted`, `bg-elevated`). Dark mode must work everywhere.
- Icons are lucide (`i-lucide-*`), mapping the design's Phosphor icons.
- Do not commit. Leave work in the tree; the user commits.

---

### Task 1: Store read `runs.latest_by_target`

One row per target component: the newest stack's latest attempt. This is what "failing job" (latest stack ended `failed`) reads.

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py` (add after `count_backfill_runs`, before `# -- Internals`)
- Test: `packages/interloper-db/tests/store/test_runs.py`

**Interfaces:**
- Produces: `RunStore.latest_by_target(org_id: UUID, *, component_kind: str | None = None) -> list[Run]`; each `Run` has `target` eager-loaded; a deleted target (`component_id IS NULL`) is excluded.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-db/tests/store/test_runs.py`:

```python
class TestLatestByTarget:
    """One run per target: the newest stack, read by its latest attempt."""

    def _run(self, store: Store, job: Component, *, status: str, created: dt.datetime, root: UUID | None = None,
             attempt: int = 1) -> Run:
        run = Run(org_id=_ORG_ID, component_id=job.id, status=status, attempt=attempt, created_at=created)
        if root is not None:
            run.root_run_id = root
        with Session(store.engine) as session:
            session.add(run)
            session.commit()
            session.refresh(run)
        if root is None:
            with Session(store.engine) as session:
                db_run = session.get(Run, run.id)
                assert db_run is not None
                db_run.root_run_id = run.id
                session.add(db_run)
                session.commit()
        return run

    def test_the_newest_stacks_latest_attempt_wins(self, store: Store) -> None:
        job = store.components.create(_ORG_ID, kind="job", key="cron_job", name="J")
        t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        old = self._run(store, job, status="success", created=t0)
        first = self._run(store, job, status="failed", created=t0 + dt.timedelta(hours=1))
        retry = self._run(store, job, status="success", created=t0 + dt.timedelta(hours=2), root=first.id, attempt=2)

        rows = store.runs.latest_by_target(_ORG_ID)

        assert [(r.id, r.status) for r in rows] == [(retry.id, "success")]
        assert old.id not in {r.id for r in rows}

    def test_kind_filter_and_org_scoping(self, store: Store) -> None:
        job = store.components.create(_ORG_ID, kind="job", key="cron_job", name="J")
        source = store.components.create(_ORG_ID, kind="source", key="demo", name="S")
        t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self._run(store, job, status="failed", created=t0)
        self._run(store, source, status="success", created=t0)

        jobs_only = store.runs.latest_by_target(_ORG_ID, component_kind="job")
        assert [r.component_id for r in jobs_only] == [job.id]
        assert store.runs.latest_by_target(uuid4()) == []

    def test_a_deleted_target_is_left_out(self, store: Store) -> None:
        t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        with Session(store.engine) as session:
            run = Run(org_id=_ORG_ID, component_id=None, status="failed", created_at=t0)
            session.add(run)
            session.commit()
            session.refresh(run)
            run.root_run_id = run.id
            session.add(run)
            session.commit()

        assert store.runs.latest_by_target(_ORG_ID) == []
```

Note: `store.components.create(_ORG_ID, kind="source", key="demo")` needs the source key to exist for `_ensure_children`; if it raises with the empty catalog, create the source row directly with `Session(store.engine)` the way `_run` does and skip `_ensure_children`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -k LatestByTarget -v`
Expected: FAIL with `AttributeError: 'RunStore' object has no attribute 'latest_by_target'`

- [ ] **Step 3: Implement**

Add to `RunStore` in `packages/interloper-db/src/interloper_db/store/runs.py`, right before `# -- Internals`:

```python
    def latest_by_target(self, org_id: UUID, *, component_kind: str | None = None) -> list[Run]:
        """The newest stack of every target, each read by its latest attempt.

        What a reader means by "the last time this job ran": the most recently
        created stack targeting the component, as its latest attempt left it.
        Runs whose target was deleted have no target to report on and are
        left out.

        Args:
            org_id: Organisation UUID.
            component_kind: Keep targets of this kind; ``None`` keeps every kind.

        Returns:
            One run per target, newest stack first, with the target loaded.
        """
        rank = (
            func.row_number()
            .over(partition_by=col(Run.component_id), order_by=col(Run.created_at).desc())
            .label("rank")
        )
        ranked = (
            select(col(Run.id), rank)
            .where(Run.org_id == org_id, col(Run.component_id).is_not(None), self._latest_attempt_only(org_id))
            .subquery()
        )
        filters: list[Any] = [col(Run.id).in_(select(ranked.c.id).where(ranked.c.rank == 1))]
        if component_kind:
            filters.append(col(Run.target).has(col(Component.kind) == component_kind))
        with session_scope(self._engine) as session:
            statement = select(Run).where(*filters).order_by(col(Run.created_at).desc()).options(*RUN_LOAD_OPTIONS)
            return list(session.exec(statement).all())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -k LatestByTarget -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check packages/interloper-db && uv run ty check packages/interloper-db`
Expected: no errors.

---

### Task 2: Store read `events.latest_by_component`

The latest event of a given set of types per component. Used for hook health (`hook_fired` / `hook_failed`).

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/events.py` (add after `latest_executions`)
- Test: `packages/interloper-db/tests/store/test_events.py`

**Interfaces:**
- Produces: `EventStore.latest_by_component(org_id: UUID, *, event_types: Sequence[str]) -> list[Event]`

- [ ] **Step 1: Write the failing test**

Append to `packages/interloper-db/tests/store/test_events.py` (module level, after `TestPartitionCoverage`):

```python
def test_latest_by_component_keeps_the_newest_event_of_the_types(store: Store) -> None:
    """One event per component: its newest of the given types, other types and orgs dropped."""
    hook_a, hook_b = uuid4(), uuid4()
    _seed([
        Event(id=uuid4(), org_id=_ORG_ID, component_id=hook_a, event_type="hook_failed", timestamp=_BASE_TS),
        Event(id=uuid4(), org_id=_ORG_ID, component_id=hook_a, event_type="hook_fired",
              timestamp=_BASE_TS + timedelta(minutes=1)),
        Event(id=uuid4(), org_id=_ORG_ID, component_id=hook_a, event_type="log",
              timestamp=_BASE_TS + timedelta(minutes=2)),
        Event(id=uuid4(), org_id=_ORG_ID, component_id=hook_b, event_type="hook_failed", timestamp=_BASE_TS),
        Event(id=uuid4(), org_id=uuid4(), component_id=uuid4(), event_type="hook_failed", timestamp=_BASE_TS),
    ])

    rows = store.events.latest_by_component(_ORG_ID, event_types=["hook_fired", "hook_failed"])

    assert {(e.component_id, e.event_type) for e in rows} == {(hook_a, "hook_fired"), (hook_b, "hook_failed")}
    assert store.events.latest_by_component(uuid4(), event_types=["hook_fired"]) == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest packages/interloper-db/tests/store/test_events.py -k latest_by_component -v`
Expected: FAIL with `AttributeError`

- [ ] **Step 3: Implement**

Add to `EventStore` after `latest_executions`:

```python
    def latest_by_component(self, org_id: UUID, *, event_types: Sequence[str]) -> list[Event]:
        """The newest event of the given types per component.

        Args:
            org_id: Organisation UUID.
            event_types: The event types that count; others are ignored.

        Returns:
            One event per component that has at least one, newest by timestamp.
        """
        rank = (
            func.row_number()
            .over(partition_by=col(Event.component_id), order_by=col(Event.timestamp).desc())
            .label("rank")
        )
        ranked = (
            select(Event, rank)
            .where(
                Event.org_id == org_id,
                col(Event.component_id).is_not(None),
                col(Event.event_type).in_(event_types),
            )
            .subquery()
        )
        latest = aliased(Event, ranked)
        with session_scope(self._engine) as session:
            return list(session.exec(select(latest).where(ranked.c.rank == 1)).all())
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest packages/interloper-db/tests/store/test_events.py -k latest_by_component -v`
Expected: PASS

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check packages/interloper-db && uv run ty check packages/interloper-db`

---

### Task 3: Store read `events.coverage_rows`

The org-wide sibling of `partition_coverage`: per job, partition and asset, whether it ever succeeded, plus one failed run id to link to. Keys of every granularity whose period overlaps the day window are read, so the route can roll hourly keys into days and monthly keys across days.

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/events.py` (new NamedTuple next to `PartitionExecution`; method after `partition_coverage`)
- Test: `packages/interloper-db/tests/store/test_events.py`

**Interfaces:**
- Produces:
  ```python
  class CoverageRow(NamedTuple):
      job_id: UUID
      partition_key: str
      asset_id: UUID
      asset_key: str | None
      succeeded: bool
      failed_run_id: UUID | None
  EventStore.coverage_rows(org_id: UUID, since: datetime.date, until: datetime.date) -> list[CoverageRow]
  ```

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-db/tests/store/test_events.py`:

```python
@pytest.mark.usefixtures("run_tables")
class TestCoverageRows:
    """Org-wide coverage: every job's partitions overlapping a day window, at any granularity."""

    def _execution(self, run_id: UUID, asset_id: UUID, status: str, *, key: str = "orders") -> None:
        with Session(engine_module.get_engine()) as session:
            session.add(Execution(run_id=run_id, component_id=asset_id, org_id=_ORG_ID, component_key=key, status=status))
            session.commit()

    def test_every_granularity_overlapping_the_window_is_read(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        day = _run(job_id=job, partition_key="2026-07-02")
        hour = _run(job_id=job, partition_key="2026-07-01T13")
        month = _run(job_id=job, partition_key="2026-06")
        year = _run(job_id=job, partition_key="2026")
        outside_day = _run(job_id=job, partition_key="2026-07-03")
        outside_month = _run(job_id=job, partition_key="2026-05")
        for run in (day, hour, month, year, outside_day, outside_month):
            self._execution(run, asset, "success")

        rows = store.events.coverage_rows(_ORG_ID, dt.date(2026, 6, 30), dt.date(2026, 7, 2))

        assert sorted(r.partition_key for r in rows) == ["2026", "2026-06", "2026-07-01T13", "2026-07-02"]
        assert all(r.job_id == job and r.asset_id == asset and r.succeeded for r in rows)

    def test_a_failed_run_is_reported_until_an_execution_succeeds(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        failed = _run(job_id=job, partition_key="2026-07-01")
        healed = _run(job_id=job, partition_key="2026-07-01")
        still_failed = _run(job_id=job, partition_key="2026-07-02")
        self._execution(failed, asset, "failed")
        self._execution(healed, asset, "success")
        self._execution(still_failed, asset, "failed")

        rows = {r.partition_key: r for r in store.events.coverage_rows(_ORG_ID, dt.date(2026, 7, 1), dt.date(2026, 7, 2))}

        assert rows["2026-07-01"].succeeded and rows["2026-07-01"].failed_run_id == failed
        assert not rows["2026-07-02"].succeeded and rows["2026-07-02"].failed_run_id == still_failed

    def test_unpartitioned_runs_deleted_jobs_and_other_orgs_stay_out(self, store: Store) -> None:
        asset = uuid4()
        self._execution(_run(job_id=uuid4(), partition_key=None), asset, "success")
        self._execution(_run(job_id=None, partition_key="2026-07-01"), asset, "success")
        self._execution(_run(org_id=uuid4(), job_id=uuid4(), partition_key="2026-07-01"), asset, "success")

        assert store.events.coverage_rows(_ORG_ID, dt.date(2026, 7, 1), dt.date(2026, 7, 2)) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-db/tests/store/test_events.py -k CoverageRows -v`
Expected: FAIL with `AttributeError`

- [ ] **Step 3: Implement**

In `packages/interloper-db/src/interloper_db/store/events.py`:

Add imports: `import datetime` (module already imports `from datetime import datetime`; add `import datetime as dt` and use `dt.date`), `from sqlalchemy import String, cast, or_`, `from interloper.partitioning.time import TimeGranularity`.

Add the NamedTuple after `PartitionExecution`:

```python
class CoverageRow(NamedTuple):
    """Whether one asset ever succeeded for one partition of one job, org-wide.

    Attributes:
        job_id: The job whose runs were read.
        partition_key: The partition, in its own granularity's key format.
        asset_id: The asset.
        asset_key: The asset's key.
        succeeded: Whether any execution of the asset for that partition succeeded.
        failed_run_id: One run whose execution of the asset failed, or ``None``.
    """

    job_id: UUID
    partition_key: str
    asset_id: UUID
    asset_key: str | None
    succeeded: bool
    failed_run_id: UUID | None
```

Add the method after `partition_coverage`:

```python
    def coverage_rows(self, org_id: UUID, since: dt.date, until: dt.date) -> list[CoverageRow]:
        """Per job, partition and asset, whether it ever succeeded, over a window of days.

        Every granularity is read: a key counts when its period overlaps the
        window, so an hourly key inside a day and a monthly key spanning it
        both list. The caller rolls them onto days.

        Args:
            org_id: Organisation UUID.
            since: First day of the window.
            until: Last day of the window, inclusive.

        Returns:
            One row per job, partition and asset that executed at least once.
        """
        end_of_until = datetime(until.year, until.month, until.day, 23)
        ranges = [
            partition_key_range(
                granularity.format(granularity.truncate(since)),
                granularity.format(end_of_until if granularity is TimeGranularity.HOUR else until),
            )
            for granularity in TimeGranularity
            if granularity.key_format is not None
        ]
        failed_run = func.min(case((col(Execution.status) == "failed", cast(col(Run.id), String))))
        statement = (
            select(
                col(Run.component_id),
                col(Run.partition_key),
                col(Execution.component_id),
                func.max(col(Execution.component_key)),
                func.max(case((col(Execution.status) == "success", 1), else_=0)),
                failed_run,
            )
            .join(Run, col(Run.id) == col(Execution.run_id))
            .where(
                Run.org_id == org_id,
                col(Run.component_id).is_not(None),
                col(Run.partition_key).is_not(None),
                or_(*(sqlalchemy.and_(*bounds) for bounds in ranges)),
            )
            .group_by(col(Run.component_id), col(Run.partition_key), col(Execution.component_id))
        )
        with session_scope(self._engine) as session:
            return [
                CoverageRow(job_id, key, asset_id, asset_key, bool(succeeded), UUID(failed) if failed else None)
                for job_id, key, asset_id, asset_key, succeeded, failed in session.exec(statement).all()
            ]
```

Use `from sqlalchemy import and_` instead of `sqlalchemy.and_`. The `cast(..., String)` makes `min()` portable: Postgres has no `min(uuid)`, and `UUID()` parses both the dashed Postgres text and SQLite's 32-hex storage.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-db/tests/store/test_events.py -k "CoverageRows or PartitionCoverage" -v`
Expected: PASS

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check packages/interloper-db && uv run ty check packages/interloper-db`

---

### Task 4: `GET /overview` route: models, health, attention, upcoming, recent, components

The route module owns the page's semantics as pure functions over store rows, then one endpoint composes them. Tests run over a real store on SQLite with a catalog built from test component classes, so drift and healthy states can both be exercised.

**Files:**
- Create: `packages/interloper-api/src/interloper_api/routes/overview.py`
- Modify: `packages/interloper-api/src/interloper_api/routes/__init__.py` (nothing to add; routers are imported in `app.py`)
- Modify: `packages/interloper-api/src/interloper_api/app.py:26-56` (import `overview`, add to `_ROUTE_MODULES` after `backfills`)
- Test: `packages/interloper-api/tests/routes/test_overview.py`

**Interfaces:**
- Consumes: `store.runs.latest_by_target`, `store.events.latest_by_component` (Tasks 1, 2); existing `store.runs.list_all/count/list_backfills/count_backfill_runs`, `store.events.error_groups/latest_executions`, `store.components.list_all/read/job_partition_granularity`; `RunResponse.from_run` from `routes/runs.py`.
- Produces the response models below, and pure helpers `hourly_buckets`, `attention_items`, `upcoming_runs`, `kind_inventory` reused by tests.

Response models (all in `routes/overview.py`):

```python
class HourBucket(BaseModel):
    hour: datetime
    succeeded: int
    failed: int

class RunsSummary(BaseModel):
    total: int
    succeeded: int
    failed: int
    hourly: list[HourBucket]

class ActivitySummary(BaseModel):
    running: int
    queued: int
    longest_running_seconds: float | None

class BackfillsSummary(BaseModel):
    active: int
    partitions_done: int
    partitions_total: int

class JobsSummary(BaseModel):
    enabled: int
    failing: int

class AttentionItem(BaseModel):
    kind: Literal["error_group", "connection", "run_stack", "drift", "overdue"]
    severity: Literal["error", "warning"]
    title: str
    target: str | None = None
    since: datetime | None = None
    component_id: UUID | None = None
    component_kind: str | None = None
    run_id: UUID | None = None

class UpcomingRun(BaseModel):
    job_id: UUID
    job_name: str
    next_run_at: datetime
    start_key: str | None = None
    end_key: str | None = None

class KindInventory(BaseModel):
    kind: str
    total: int
    healthy: int
    failing: int
    attention: int
    disabled: int

class OverviewResponse(BaseModel):
    generated_at: datetime
    runs: RunsSummary
    activity: ActivitySummary
    backfills: BackfillsSummary
    jobs: JobsSummary
    attention: list[AttentionItem]
    upcoming: list[UpcomingRun]
    recent: list[RunResponse]
    components: list[KindInventory]
```

- [ ] **Step 1: Write the failing tests**

Create `packages/interloper-api/tests/routes/test_overview.py`:

```python
"""Tests for ``interloper_api.routes.overview``.

A real store over in-memory SQLite, with a catalog of test component classes
so both live and drifted rows exist, and the semantics (failing, overdue,
drift, renewal errors, state precedence, coverage day rules) are exercised
against real rows.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from functools import cached_property
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import engine as engine_module
from interloper_db.models import (
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Execution,
    Organisation,
    Profile,
    Quota,
    Run,
    Usage,
    UserOrganisation,
)
from interloper_db.store import Store
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer
from interloper_api.routes import overview as overview_module

NOW = dt.datetime(2026, 8, 13, 9, 14, tzinfo=dt.timezone.utc)


@il.connection(name="Shop API")
class ShopConnection(il.Connection):
    api_key: str = il.SecretField(default="k")

    @cached_property
    def client(self) -> None:
        return None


class Order(il.Schema):
    id: int


@il.source
class Shop(il.Source):
    @il.asset(schema=Order, partitioning=il.TimePartitionConfig(column="date"))
    def orders(self) -> list[dict]:
        return []


@pytest.fixture
def db() -> Iterator[Engine]:
    eng = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(eng, "connect")
    def _configure(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    models = (Profile, Organisation, UserOrganisation, Component, ComponentRelation, Backfill, Run, Event, Quota, Usage)
    for model in (*models, Execution):
        model.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        yield eng
    finally:
        eng.dispose()
        engine_module._engine = None


@pytest.fixture
def store(db: Engine) -> Store:
    return Store(catalog=il.Catalog.from_assets([Shop, ShopConnection]), encrypt=lambda b: b, decrypt=lambda b: b)


@pytest.fixture
def member(store: Store) -> SimpleNamespace:
    profile = store.auth.upsert_profile(google_id="g-1", email="ada@example.com", name="Ada")
    org = store.organisations.create(name="Acme", creator_id=profile.id)
    return SimpleNamespace(id=profile.id, org_id=org.id, email="ada@example.com", is_super_admin=False)


@pytest.fixture
def client(store: Store, member: SimpleNamespace) -> TestClient:
    app = FastAPI()
    app.include_router(overview_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: member
    app.dependency_overrides[require_viewer] = lambda: member
    app.dependency_overrides[get_org_id] = lambda: member.org_id
    return TestClient(app)


def _run(store: Store, org_id: UUID, target: Component | None, *, status: str, started: dt.datetime | None,
         completed: dt.datetime | None, partition_key: str | None = None, backfill_id: UUID | None = None,
         attempt: int = 1, root: UUID | None = None) -> Run:
    run = Run(
        org_id=org_id,
        component_id=target.id if target else None,
        status=status,
        started_at=started,
        completed_at=completed,
        created_at=started or NOW,
        partition_key=partition_key,
        backfill_id=backfill_id,
        attempt=attempt,
    )
    with Session(store.engine) as session:
        session.add(run)
        session.commit()
        session.refresh(run)
        run.root_run_id = root or run.id
        session.add(run)
        session.commit()
        session.refresh(run)
    return run


def _stamp(store: Store, component: Component, **state: Any) -> None:
    store.components.stamp_state(component.id, **state)


def _execution(store: Store, run: Run, asset: Component, status: str) -> None:
    with Session(store.engine) as session:
        session.add(
            Execution(run_id=run.id, component_id=asset.id, org_id=run.org_id, component_key=asset.key, status=status,
                      created_at=run.created_at)
        )
        session.commit()


def _job(store: Store, org_id: UUID, name: str, *, enabled: bool = True, targets: list[UUID] | None = None,
         cron: str = "0 4 * * *") -> Component:
    return store.components.create(
        org_id,
        kind="job",
        key="cron_job",
        name=name,
        config={"cron": cron, "enabled": enabled},
        relations={"targets": targets or []},
    )


class TestHealthStrip:
    def test_runs_last_24h_bucket_by_completion_hour(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        _run(store, member.org_id, job, status="success", started=NOW - dt.timedelta(hours=2),
             completed=NOW - dt.timedelta(hours=1, minutes=50))
        _run(store, member.org_id, job, status="failed", started=NOW - dt.timedelta(hours=2),
             completed=NOW - dt.timedelta(hours=1, minutes=40))
        _run(store, member.org_id, job, status="success", started=NOW - dt.timedelta(days=2),
             completed=NOW - dt.timedelta(days=2))

        body = client.get("/overview", params={"now": NOW.isoformat()}).json()

        assert body["runs"]["total"] == 2
        assert body["runs"]["succeeded"] == 1 and body["runs"]["failed"] == 1
        assert len(body["runs"]["hourly"]) == 24
        assert sum(b["succeeded"] + b["failed"] for b in body["runs"]["hourly"]) == 2

    def test_activity_counts_running_and_queued(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        _run(store, member.org_id, job, status="running", started=NOW - dt.timedelta(minutes=5), completed=None)
        _run(store, member.org_id, job, status="queued", started=None, completed=None)
        _run(store, member.org_id, job, status="queued", started=None, completed=None)

        body = client.get("/overview", params={"now": NOW.isoformat()}).json()

        assert body["activity"] == {"running": 1, "queued": 2, "longest_running_seconds": 300.0}

    def test_backfills_in_progress_roll_up_their_partitions(self, client: TestClient, store: Store,
                                                            member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        backfill = store.runs.create_backfill(member.org_id, component_id=job.id, start_key="2026-07-01",
                                              end_key="2026-07-04")
        with Session(store.engine) as session:
            for run in session.exec(il_select_runs(backfill.id)).all():  # see helper below
                pass

        body = client.get("/overview", params={"now": NOW.isoformat()}).json()

        assert body["backfills"]["active"] == 1
        assert body["backfills"]["partitions_total"] == 4
        assert body["backfills"]["partitions_done"] == 0

    def test_jobs_failing_reads_the_latest_stack(self, client: TestClient, store: Store, member: SimpleNamespace):
        healthy = _job(store, member.org_id, "healthy")
        failing = _job(store, member.org_id, "failing")
        _job(store, member.org_id, "off", enabled=False)
        first = _run(store, member.org_id, failing, status="failed", started=NOW - dt.timedelta(hours=3),
                     completed=NOW - dt.timedelta(hours=3))
        _run(store, member.org_id, failing, status="failed", started=NOW - dt.timedelta(hours=2),
             completed=NOW - dt.timedelta(hours=2), attempt=2, root=first.id)
        _run(store, member.org_id, healthy, status="failed", started=NOW - dt.timedelta(hours=3),
             completed=NOW - dt.timedelta(hours=3))
        _run(store, member.org_id, healthy, status="success", started=NOW - dt.timedelta(hours=1),
             completed=NOW - dt.timedelta(hours=1))

        body = client.get("/overview", params={"now": NOW.isoformat()}).json()

        assert body["jobs"] == {"enabled": 2, "failing": 1}
```

Replace the `test_backfills_in_progress_roll_up_their_partitions` body's `with Session ... pass` block with nothing (delete those three lines); `create_backfill` already fans out 4 queued runs, which is what "0 done of 4" asserts. Then continue the file:

```python
class TestAttention:
    def test_error_groups_merge_on_job_and_first_line(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "meta-ads-daily")
        for i in range(3):
            run = _run(store, member.org_id, job, status="failed", started=NOW - dt.timedelta(hours=1 + i),
                       completed=NOW - dt.timedelta(hours=1 + i))
            with Session(store.engine) as session:
                session.add(Event(id=uuid4(), org_id=member.org_id, run_id=run.id, event_type="run_failed",
                                  error=f"token expired\ndetail {i}", timestamp=NOW - dt.timedelta(hours=1 + i)))
                session.commit()

        items = client.get("/overview", params={"now": NOW.isoformat()}).json()["attention"]
        groups = [i for i in items if i["kind"] == "error_group"]

        assert len(groups) == 1
        assert groups[0]["title"] == "3 runs failed: token expired"
        assert groups[0]["target"] == "meta-ads-daily"
        assert groups[0]["severity"] == "error"
        assert groups[0]["run_id"] is not None

    def test_a_connection_with_a_renewal_error_needs_reauthorisation(self, client: TestClient, store: Store,
                                                                     member: SimpleNamespace):
        connection = store.components.create(member.org_id, kind="connection", key=ShopConnection.key,
                                             name="tiktok-ads", config={"api_key": "k"})
        _stamp(store, connection, last_renewal_error="invalid_grant: token revoked")

        items = client.get("/overview", params={"now": NOW.isoformat()}).json()["attention"]
        item = next(i for i in items if i["kind"] == "connection")

        assert item["title"] == "tiktok-ads connection needs re-authorisation"
        assert item["target"] == "invalid_grant: token revoked"
        assert item["component_id"] == str(connection.id)

    def test_a_stack_still_failing_after_retries(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "google-ads-daily")
        first = _run(store, member.org_id, job, status="failed", started=NOW - dt.timedelta(hours=3),
                     completed=NOW - dt.timedelta(hours=3), partition_key="2026-08-11")
        _run(store, member.org_id, job, status="failed", started=NOW - dt.timedelta(hours=2),
             completed=NOW - dt.timedelta(hours=2), partition_key="2026-08-11", attempt=3, root=first.id)

        items = client.get("/overview", params={"now": NOW.isoformat()}).json()["attention"]
        item = next(i for i in items if i["kind"] == "run_stack")

        assert item["title"] == "Still failing after 3 attempts"
        assert item["target"] == "google-ads-daily · 2026-08-11"

    def test_drift_and_overdue_are_warnings(self, client: TestClient, store: Store, member: SimpleNamespace):
        with Session(store.engine) as session:
            session.add(Component(org_id=member.org_id, kind="source", key="gone", name="Old source"))
            session.commit()
        overdue = _job(store, member.org_id, "bing-ads-daily")
        _stamp(store, overdue, next_run_at=NOW - dt.timedelta(hours=5, minutes=12))
        on_time = _job(store, member.org_id, "fine")
        _stamp(store, on_time, next_run_at=NOW - dt.timedelta(minutes=5))

        items = client.get("/overview", params={"now": NOW.isoformat()}).json()["attention"]
        by_kind = {i["kind"]: i for i in items}

        assert by_kind["drift"]["title"] == "Old source is missing from the catalog"
        assert by_kind["drift"]["severity"] == "warning"
        assert by_kind["overdue"]["title"] == "Scheduled run is 5h 12m overdue"
        assert by_kind["overdue"]["target"] == "bing-ads-daily"
        assert [i["kind"] for i in items if i["kind"] == "overdue"] == ["overdue"]

    def test_errors_sort_before_warnings(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        _stamp(store, job, next_run_at=NOW - dt.timedelta(hours=1))
        connection = store.components.create(member.org_id, kind="connection", key=ShopConnection.key, name="c",
                                             config={"api_key": "k"})
        _stamp(store, connection, last_renewal_error="bad")

        items = client.get("/overview", params={"now": NOW.isoformat()}).json()["attention"]

        assert [i["severity"] for i in items] == ["error", "warning"]


class TestUpcomingAndRecent:
    def test_upcoming_lists_enabled_jobs_by_next_firing_with_their_window(self, client: TestClient, store: Store,
                                                                         member: SimpleNamespace):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        later = _job(store, member.org_id, "later", targets=[source.id])
        sooner = _job(store, member.org_id, "sooner", targets=[source.id])
        unpartitioned = _job(store, member.org_id, "plain")
        _job(store, member.org_id, "off", enabled=False)
        _stamp(store, later, next_run_at=NOW + dt.timedelta(hours=7))
        _stamp(store, sooner, next_run_at=NOW + dt.timedelta(hours=4))
        _stamp(store, unpartitioned, next_run_at=NOW + dt.timedelta(hours=5))

        upcoming = client.get("/overview", params={"now": NOW.isoformat()}).json()["upcoming"]

        assert [u["job_name"] for u in upcoming] == ["sooner", "plain", "later"]
        assert upcoming[0]["start_key"] == "2026-08-12" and upcoming[0]["end_key"] == "2026-08-12"
        assert upcoming[1]["start_key"] is None

    def test_recent_lists_the_latest_five_stacks(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        for i in range(7):
            _run(store, member.org_id, job, status="success", started=NOW - dt.timedelta(hours=i + 1),
                 completed=NOW - dt.timedelta(hours=i + 1), partition_key=f"2026-08-{i + 1:02d}")

        recent = client.get("/overview", params={"now": NOW.isoformat()}).json()["recent"]

        assert [r["partition_key"] for r in recent] == ["2026-08-01", "2026-08-02", "2026-08-03", "2026-08-04",
                                                        "2026-08-05"]


class TestComponents:
    def test_each_kind_counts_its_states_with_precedence(self, client: TestClient, store: Store,
                                                        member: SimpleNamespace):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        failing_job = _job(store, member.org_id, "failing")
        _job(store, member.org_id, "off", enabled=False)
        run = _run(store, member.org_id, failing_job, status="failed", started=NOW - dt.timedelta(hours=1),
                   completed=NOW - dt.timedelta(hours=1))
        _execution(store, run, asset, "failed")
        connection = store.components.create(member.org_id, kind="connection", key=ShopConnection.key, name="c",
                                             config={"api_key": "k"})
        _stamp(store, connection, last_renewal_error="bad")
        with Session(store.engine) as session:
            session.add(Component(org_id=member.org_id, kind="destination", key="gone", name="old"))
            session.commit()

        rows = {r["kind"]: r for r in client.get("/overview", params={"now": NOW.isoformat()}).json()["components"]}

        assert rows["source"] == {"kind": "source", "total": 1, "healthy": 0, "failing": 1, "attention": 0, "disabled": 0}
        assert rows["asset"]["failing"] == 1
        assert rows["job"] == {"kind": "job", "total": 2, "healthy": 0, "failing": 1, "attention": 0, "disabled": 1}
        assert rows["connection"]["attention"] == 1
        assert rows["destination"]["attention"] == 1
        assert rows["hook"]["total"] == 0
        assert [r["kind"] for r in rows.values()] == ["source", "asset", "destination", "connection", "job", "hook"]


class TestAuth:
    def test_a_viewer_is_required(self, store: Store, member: SimpleNamespace):
        app = FastAPI()
        app.include_router(overview_module.router)
        app.dependency_overrides[get_store] = lambda: store
        app.dependency_overrides[get_org_id] = lambda: member.org_id

        assert TestClient(app).get("/overview").status_code in (401, 403)
```

Also remove the `il_select_runs` reference by deleting the `with Session` block in the backfills test as instructed above.

Notes for the implementer:
- `Shop.key` / `ShopConnection.key` are the class-level catalog keys; use them rather than guessing strings.
- `store.components.create(... kind="source", key=Shop.key)` creates the `orders` child asset through `_ensure_children`; `source.children[0]` is it.
- `?now=` is a test seam: the endpoint takes an optional `now` query parameter (ISO datetime) defaulting to the current UTC time, so assertions are deterministic. Document it as such on the endpoint.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-api/tests/routes/test_overview.py -v`
Expected: FAIL at import: `cannot import name 'overview'`

- [ ] **Step 3: Implement the route module**

Create `packages/interloper-api/src/interloper_api/routes/overview.py`:

```python
"""Overview API: the landing page's health, attention, schedule and inventory in one read.

The store answers what the data says; this module answers what it means
for the reader, as pure functions over store rows, so every tile and list
on the page shares one vocabulary (see ``docs/superpowers/specs/2026-09-30-overview-page-design.md``).
"""

from __future__ import annotations

import datetime as dt
from collections import Counter, defaultdict
from typing import Any, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Query
from interloper.partitioning.time import TimePartitionWindow
from interloper_db import Component, ComponentStatus, Store
from interloper_db.models import Run
from interloper_db.store.events import ErrorGroup
from pydantic import BaseModel
from sqlmodel import Session

from interloper_api.dependencies import OrgIdDep, StoreDep, ViewerDep
from interloper_api.routes.runs import RunResponse

router = APIRouter(prefix="/overview", tags=["overview"])

#: A scheduled slot the scheduler has not picked up after this long is overdue.
OVERDUE_AFTER = dt.timedelta(minutes=15)
#: Event types whose error text records one failure verdict each.
FAILURE_EVENT_TYPES = ("operation_failed", "run_failed")
#: Kinds the inventory reports, in the design's order.
INVENTORY_KINDS = ("source", "asset", "destination", "connection", "job", "hook")
RECENT_LIMIT = 5
TERMINAL_STATUSES = frozenset({"success", "failed", "canceled"})


# -- Response models -----------------------------------------------------------


class HourBucket(BaseModel):
    """Attempts that finished inside one hour, by verdict."""

    hour: dt.datetime
    succeeded: int
    failed: int


class RunsSummary(BaseModel):
    """Attempts finished in the last 24 hours, with their hourly profile."""

    total: int
    succeeded: int
    failed: int
    hourly: list[HourBucket]


class ActivitySummary(BaseModel):
    """What is executing right now."""

    running: int
    queued: int
    longest_running_seconds: float | None = None


class BackfillsSummary(BaseModel):
    """Backfills still queued or running, their partitions rolled up."""

    active: int
    partitions_done: int
    partitions_total: int


class JobsSummary(BaseModel):
    """Enabled jobs, and how many of them last ended in failure."""

    enabled: int
    failing: int


class AttentionItem(BaseModel):
    """One thing that needs a person, with what to open to act on it."""

    kind: Literal["error_group", "connection", "run_stack", "drift", "overdue"]
    severity: Literal["error", "warning"]
    title: str
    target: str | None = None
    since: dt.datetime | None = None
    component_id: UUID | None = None
    component_kind: str | None = None
    run_id: UUID | None = None


class UpcomingRun(BaseModel):
    """A job's next firing and the partition window it will cover."""

    job_id: UUID
    job_name: str
    next_run_at: dt.datetime
    start_key: str | None = None
    end_key: str | None = None


class KindInventory(BaseModel):
    """How many components of one kind sit in each state."""

    kind: str
    total: int
    healthy: int
    failing: int
    attention: int
    disabled: int


class OverviewResponse(BaseModel):
    """Everything the overview page draws, except the coverage calendar."""

    generated_at: dt.datetime
    runs: RunsSummary
    activity: ActivitySummary
    backfills: BackfillsSummary
    jobs: JobsSummary
    attention: list[AttentionItem]
    upcoming: list[UpcomingRun]
    recent: list[RunResponse]
    components: list[KindInventory]


# -- Semantics -----------------------------------------------------------------


def hourly_buckets(runs: list[Run], now: dt.datetime) -> list[HourBucket]:
    """Bucket finished attempts by the hour they completed in, over the last 24 hours.

    Args:
        runs: Attempts that completed in the window.
        now: The window's end.

    Returns:
        24 buckets, oldest first, each covering one clock hour.
    """
    end = now.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
    start = end - dt.timedelta(hours=24)
    counts: dict[dt.datetime, Counter[str]] = defaultdict(Counter)
    for run in runs:
        if run.completed_at is None or run.status not in ("success", "failed"):
            continue
        hour = run.completed_at.astimezone(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
        if start <= hour < end:
            counts[hour][run.status] += 1
    return [
        HourBucket(hour=hour, succeeded=counts[hour]["success"], failed=counts[hour]["failed"])
        for hour in (start + dt.timedelta(hours=i) for i in range(24))
    ]


def enabled(component: Component) -> bool:
    """Whether a component's config leaves it enabled (the default).

    Args:
        component: The row to read.

    Returns:
        ``False`` only when the config says ``enabled: false``.
    """
    return (component.config or {}).get("enabled", True)


def error_group_items(groups: list[ErrorGroup], names: dict[UUID, str]) -> list[AttentionItem]:
    """Merge error groups by job and the error's first line into attention items.

    Args:
        groups: The store's error groups (one per job, run, component, type and text).
        names: Job names by id.

    Returns:
        One item per job and first line, loudest first.
    """
    merged: dict[tuple[UUID | None, str], dict[str, Any]] = {}
    for group in groups:
        line = group.error.splitlines()[0].strip() if group.error else ""
        entry = merged.setdefault(
            (group.job_id, line), {"runs": set(), "last_seen": group.last_seen, "sample": group.run_id}
        )
        entry["runs"].add(group.run_id)
        if group.last_seen > entry["last_seen"]:
            entry["last_seen"] = group.last_seen
            entry["sample"] = group.run_id
    items = []
    for (job_id, line), entry in merged.items():
        count = len(entry["runs"])
        items.append(
            AttentionItem(
                kind="error_group",
                severity="error",
                title=f"{count} run{'s' if count != 1 else ''} failed: {line}",
                target=names.get(job_id) if job_id else None,
                since=entry["last_seen"],
                component_id=job_id,
                component_kind="job",
                run_id=entry["sample"],
            )
        )
    return sorted(items, key=lambda i: (-len(merged[(i.component_id, i.title.split(': ', 1)[1])]["runs"]), i.title))


def connection_items(connections: list[Component]) -> list[AttentionItem]:
    """Connections whose last renewal failed.

    Args:
        connections: The organisation's connection rows.

    Returns:
        One item per connection carrying ``state.last_renewal_error``.
    """
    items = []
    for connection in connections:
        state = connection.state or {}
        error = state.get("last_renewal_error")
        if not error:
            continue
        renewed = state.get("last_renewed_at")
        items.append(
            AttentionItem(
                kind="connection",
                severity="error",
                title=f"{connection.name or connection.key} connection needs re-authorisation",
                target=str(error).splitlines()[0],
                since=dt.datetime.fromisoformat(renewed) if renewed else None,
                component_id=connection.id,
                component_kind="connection",
            )
        )
    return items


def run_stack_items(latest: list[Run], names: dict[UUID, str], since: dt.datetime) -> list[AttentionItem]:
    """Stacks whose latest attempt is a retry that still failed, finished after *since*.

    Args:
        latest: The latest attempt of the newest stack per target.
        names: Job names by id.
        since: Only stacks that finished at or after this instant count.

    Returns:
        One item per stack.
    """
    items = []
    for run in latest:
        if run.status != "failed" or run.attempt <= 1 or run.completed_at is None or run.completed_at < since:
            continue
        name = names.get(run.component_id) if run.component_id else None
        target = f"{name} · {run.partition_key}" if run.partition_key else name
        items.append(
            AttentionItem(
                kind="run_stack",
                severity="error",
                title=f"Still failing after {run.attempt} attempts",
                target=target,
                since=run.completed_at,
                component_id=run.component_id,
                component_kind="job",
                run_id=run.id,
            )
        )
    return items


DRIFT_TITLES = {
    ComponentStatus.MISSING: "{name} is missing from the catalog",
    ComponentStatus.DISABLED: "{name} is disabled in this deployment",
    ComponentStatus.UNREADABLE: "{name} has an unreadable config",
}


def drift_items(statuses: list[tuple[Component, ComponentStatus]]) -> list[AttentionItem]:
    """Components whose catalog status is not ``ok``.

    Args:
        statuses: Every component row with its read status.

    Returns:
        One warning per drifted component.
    """
    return [
        AttentionItem(
            kind="drift",
            severity="warning",
            title=DRIFT_TITLES[status].format(name=component.name or component.key),
            target=component.key,
            component_id=component.id,
            component_kind=component.kind,
        )
        for component, status in statuses
        if status is not ComponentStatus.OK
    ]


def overdue_items(jobs: list[Component], now: dt.datetime) -> list[AttentionItem]:
    """Enabled jobs whose next slot passed more than :data:`OVERDUE_AFTER` ago.

    Args:
        jobs: The organisation's job rows.
        now: The reference instant.

    Returns:
        One warning per overdue job.
    """
    items = []
    for job in jobs:
        if not enabled(job):
            continue
        due = _state_datetime(job, "next_run_at")
        if due is None or now - due <= OVERDUE_AFTER:
            continue
        items.append(
            AttentionItem(
                kind="overdue",
                severity="warning",
                title=f"Scheduled run is {_duration(now - due)} overdue",
                target=job.name or job.key,
                since=due,
                component_id=job.id,
                component_kind="job",
            )
        )
    return items


def upcoming_runs(store: Store, jobs: list[Component]) -> list[UpcomingRun]:
    """Enabled jobs by their next firing, each with the window that firing covers.

    The window is resolved the way the scheduler resolves it: the job's
    ``lookback`` and ``offset`` over its targets' granularity, on the job
    timezone's clock at the firing instant.

    Args:
        store: The store the targets' granularity is resolved through.
        jobs: The organisation's job rows.

    Returns:
        Upcoming firings, soonest first; jobs without a stored next slot are absent.
    """
    upcoming = []
    with Session(store.engine) as session:
        for job in jobs:
            next_run = _state_datetime(job, "next_run_at")
            if not enabled(job) or next_run is None:
                continue
            config = job.config or {}
            window = None
            lookback = config.get("lookback", 1)
            try:
                granularity = store.components.job_partition_granularity(session, job.id) if lookback else None
            except ValueError:
                granularity = None
            if granularity is not None:
                window = TimePartitionWindow.lookback(
                    next_run.astimezone(_zone(config.get("timezone"))),
                    lookback=lookback,
                    offset=config.get("offset", 1),
                    granularity=granularity,
                )
            upcoming.append(
                UpcomingRun(
                    job_id=job.id,
                    job_name=job.name or job.key,
                    next_run_at=next_run,
                    start_key=window.granularity.format(window.start) if window else None,
                    end_key=window.granularity.format(window.end) if window else None,
                )
            )
    return sorted(upcoming, key=lambda u: u.next_run_at)


def kind_inventory(
    statuses: list[tuple[Component, ComponentStatus]],
    failing_ids: set[UUID],
    attention_ids: set[UUID],
) -> list[KindInventory]:
    """Count each kind's components by state, first match winning: disabled, failing, attention, healthy.

    Args:
        statuses: Every component row with its read status.
        failing_ids: Components whose latest execution, stack or hook event failed.
        attention_ids: Components needing attention for a reason other than drift.

    Returns:
        One row per kind in :data:`INVENTORY_KINDS`.
    """
    counts: dict[str, Counter[str]] = {kind: Counter() for kind in INVENTORY_KINDS}
    for component, status in statuses:
        if component.kind not in counts:
            continue
        if not enabled(component):
            state = "disabled"
        elif component.id in failing_ids:
            state = "failing"
        elif status is not ComponentStatus.OK or component.id in attention_ids:
            state = "attention"
        else:
            state = "healthy"
        counts[component.kind][state] += 1
    return [
        KindInventory(kind=kind, total=sum(c.values()), healthy=c["healthy"], failing=c["failing"],
                      attention=c["attention"], disabled=c["disabled"])
        for kind, c in counts.items()
    ]


# -- Helpers -------------------------------------------------------------------


def _state_datetime(component: Component, key: str) -> dt.datetime | None:
    """Read a UTC timestamp out of a component's machine-owned state.

    Args:
        component: The row whose state is read.
        key: The state key holding an ISO-8601 timestamp.

    Returns:
        The aware datetime, or ``None`` when absent.
    """
    value = (component.state or {}).get(key)
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _zone(name: str | None) -> dt.tzinfo:
    """Resolve a job timezone name, falling back to UTC like the scheduler does.

    Args:
        name: The IANA name from the job's config, or ``None``.

    Returns:
        The zone.
    """
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        return dt.timezone.utc


def _duration(delta: dt.timedelta) -> str:
    """Render a duration as ``5h 12m`` / ``12m`` / ``2d 3h``.

    Args:
        delta: The duration.

    Returns:
        The compact label.
    """
    minutes = int(delta.total_seconds() // 60)
    days, minutes = divmod(minutes, 24 * 60)
    hours, minutes = divmod(minutes, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _read_statuses(store: Store, components: list[Component]) -> list[tuple[Component, ComponentStatus]]:
    """Read every component's status once, owned rows through their owner's key.

    Args:
        store: The store to read through.
        components: Every row of the organisation.

    Returns:
        Each row paired with its status.
    """
    keys = {component.id: component.key for component in components}
    return [
        (component, store.components.read(component, parent_key=keys.get(component.parent_id) if component.parent_id else None).status)
        for component in components
    ]


# -- Endpoint ------------------------------------------------------------------


@router.get("")
def get_overview(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    now: dt.datetime | None = Query(default=None, description="Reference instant; defaults to the current time"),
) -> OverviewResponse:
    """Compose the overview page's data for the current organisation.

    Args:
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        now: Reference instant, overridable so the read is reproducible.

    Returns:
        The overview.
    """
    now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    day_ago = now - dt.timedelta(hours=24)

    components = store.components.list_all(org_id)
    statuses = _read_statuses(store, components)
    jobs = [c for c in components if c.kind == "job"]
    names = {job.id: job.name or job.key for job in jobs}

    finished = store.runs.list_all(org_id, after=day_ago, before=now, all_attempts=True, limit=100_000)
    running = store.runs.list_all(org_id, status="running", limit=10_000)
    queued = store.runs.count(org_id, status="queued")
    longest = max(((now - r.started_at).total_seconds() for r in running if r.started_at), default=None)

    backfills = store.runs.list_backfills(org_id, active_only=True)
    backfill_counts = store.runs.count_backfill_runs([b.id for b in backfills])
    done = sum(
        sum(n for status, n in backfill_counts.get(b.id, {}).items() if status in TERMINAL_STATUSES) for b in backfills
    )

    latest = store.runs.latest_by_target(org_id)
    latest_by_job = {run.component_id: run for run in latest if run.target and run.target.kind == "job"}
    enabled_jobs = [job for job in jobs if enabled(job)]
    failing_jobs = {job.id for job in enabled_jobs if (run := latest_by_job.get(job.id)) and run.status == "failed"}

    groups, _ = store.events.error_groups(org_id, event_types=FAILURE_EVENT_TYPES, since=day_ago, until=now)
    attention = [
        *error_group_items(groups, names),
        *connection_items([c for c in components if c.kind == "connection"]),
        *run_stack_items(list(latest_by_job.values()), names, day_ago),
        *drift_items(statuses),
        *overdue_items(jobs, now),
    ]
    attention.sort(key=lambda i: (i.severity != "error", -(i.since.timestamp() if i.since else 0)))

    failing_assets = {e.component_id for e in store.events.latest_executions(org_id) if e.status == "failed"}
    failing_sources = {c.parent_id for c in components if c.kind == "asset" and c.id in failing_assets and c.parent_id}
    failing_hooks = {
        e.component_id
        for e in store.events.latest_by_component(org_id, event_types=("hook_fired", "hook_failed"))
        if e.event_type == "hook_failed" and e.component_id
    }
    attention_ids = {i.component_id for i in attention if i.kind == "connection" and i.component_id}

    return OverviewResponse(
        generated_at=now,
        runs=RunsSummary(
            total=sum(1 for r in finished if r.status in ("success", "failed")),
            succeeded=sum(1 for r in finished if r.status == "success"),
            failed=sum(1 for r in finished if r.status == "failed"),
            hourly=hourly_buckets(finished, now),
        ),
        activity=ActivitySummary(running=len(running), queued=queued, longest_running_seconds=longest),
        backfills=BackfillsSummary(
            active=len(backfills), partitions_done=done, partitions_total=sum(b.partitions for b in backfills)
        ),
        jobs=JobsSummary(enabled=len(enabled_jobs), failing=len(failing_jobs)),
        attention=attention,
        upcoming=upcoming_runs(store, jobs),
        recent=[RunResponse.from_run(run) for run in store.runs.list_all(org_id, limit=RECENT_LIMIT)],
        components=kind_inventory(statuses, failing_jobs | failing_assets | failing_sources | failing_hooks, attention_ids),
    )
```

Then in `app.py`, add `overview` to the `from interloper_api.routes import (...)` block and to `_ROUTE_MODULES` after `backfills`.

Implementation notes:
- `runs.list_all(after=day_ago, before=now, all_attempts=True)` selects runs *overlapping* the window (see `_run_filters`); `hourly_buckets` and the totals then keep only those that completed inside it. A run that completed before `day_ago` but started inside is impossible, so filtering on `completed_at >= day_ago` in `RunsSummary` totals is what "finished in the last 24h" means: apply `r.completed_at is not None and r.completed_at >= day_ago` to the three `sum(...)` expressions (define `finished = [r for r in finished if r.completed_at and r.completed_at >= day_ago]` once before use).
- `error_group_items` sorting: simplify to sorting by `-count` then title by keeping the count in a local list of `(count, item)` tuples instead of re-deriving it from the title. Rewrite the return as `return [item for _, item in sorted(pairs, key=lambda p: (-p[0], p[1].title))]`.
- `ErrorGroup` is exported from `interloper_db.store.events`; confirm `Component`, `ComponentStatus` and `Store` import from `interloper_db` (they do in `routes/components.py`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-api/tests/routes/test_overview.py -v`
Expected: PASS. If `store.components.create(kind="job", key="cron_job", relations={"targets": []})` rejects an empty target list, pass `relations=None` when `targets` is falsy in `_job`.

- [ ] **Step 5: Lint and type check**

Run: `uv run ruff check packages/interloper-api && uv run ty check packages/interloper-api`
Expected: clean. Wrap the long `_read_statuses` comprehension to 120 columns.

---

### Task 5: `GET /overview/coverage` route

Per job and day, how many asset-partitions were expected, covered and failed, with a failed run to open. Day rules from the spec, applied in the route over `events.coverage_rows`.

**Files:**
- Modify: `packages/interloper-api/src/interloper_api/routes/overview.py`
- Test: `packages/interloper-api/tests/routes/test_overview.py`

**Interfaces:**
- Consumes: `store.events.coverage_rows(org_id, since, until)` (Task 3).
- Produces:
  ```python
  class CoverageDay(BaseModel):
      date: dt.date
      job_id: UUID
      expected: int
      covered: int
      failed: int
      failed_run_id: UUID | None = None

  class CoverageJob(BaseModel):
      id: UUID
      name: str

  class CoverageResponse(BaseModel):
      since: dt.date
      until: dt.date
      jobs: list[CoverageJob]
      days: list[CoverageDay]

  def coverage_days(rows: list[CoverageRow], jobs: list[Component], since: dt.date, until: dt.date, today: dt.date) -> list[CoverageDay]
  ```

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-api/tests/routes/test_overview.py`:

```python
class TestCoverage:
    def _partition_run(self, store: Store, member: SimpleNamespace, job: Component, asset: Component, key: str,
                       status: str) -> Run:
        run = _run(store, member.org_id, job, status=status, started=NOW - dt.timedelta(days=1),
                   completed=NOW - dt.timedelta(days=1), partition_key=key)
        _execution(store, run, asset, status)
        return run

    def test_days_run_from_the_first_attempt_to_yesterday(self, client: TestClient, store: Store,
                                                          member: SimpleNamespace):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "daily", targets=[source.id])
        self._partition_run(store, member, job, asset, "2026-08-10", "success")
        failed = self._partition_run(store, member, job, asset, "2026-08-11", "failed")

        body = client.get("/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-13",
                                                        "now": NOW.isoformat()}).json()
        days = {d["date"]: d for d in body["days"]}

        assert body["jobs"] == [{"id": str(job.id), "name": "daily"}]
        assert sorted(days) == ["2026-08-10", "2026-08-11", "2026-08-12"]
        assert days["2026-08-10"] == {"date": "2026-08-10", "job_id": str(job.id), "expected": 1, "covered": 1,
                                      "failed": 0, "failed_run_id": None}
        assert days["2026-08-11"]["failed"] == 1 and days["2026-08-11"]["failed_run_id"] == str(failed.id)
        assert days["2026-08-12"] == {"date": "2026-08-12", "job_id": str(job.id), "expected": 1, "covered": 0,
                                      "failed": 0, "failed_run_id": None}

    def test_today_counts_once_attempted_and_a_disabled_job_stops_at_its_last_partition(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        live = _job(store, member.org_id, "live", targets=[source.id])
        off = _job(store, member.org_id, "off", enabled=False, targets=[source.id])
        self._partition_run(store, member, live, asset, "2026-08-13", "success")
        self._partition_run(store, member, off, asset, "2026-08-05", "success")

        body = client.get("/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-13",
                                                        "now": NOW.isoformat()}).json()
        by_job = defaultdict(list)
        for day in body["days"]:
            by_job[day["job_id"]].append(day["date"])

        assert by_job[str(live.id)] == ["2026-08-13"]
        assert by_job[str(off.id)] == ["2026-08-05"]

    def test_hourly_and_monthly_keys_roll_onto_days(self, client: TestClient, store: Store, member: SimpleNamespace):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        hourly = _job(store, member.org_id, "hourly", targets=[source.id])
        monthly = _job(store, member.org_id, "monthly", targets=[source.id])
        self._partition_run(store, member, hourly, asset, "2026-08-12T00", "success")
        self._partition_run(store, member, hourly, asset, "2026-08-12T01", "failed")
        self._partition_run(store, member, monthly, asset, "2026-07", "success")

        body = client.get("/overview/coverage", params={"since": "2026-07-30", "until": "2026-08-13",
                                                        "now": NOW.isoformat()}).json()
        days = {(d["job_id"], d["date"]): d for d in body["days"]}

        hour_day = days[(str(hourly.id), "2026-08-12")]
        assert (hour_day["expected"], hour_day["covered"], hour_day["failed"]) == (24, 1, 1)
        assert days[(str(monthly.id), "2026-07-30")]["covered"] == 1
        assert days[(str(monthly.id), "2026-07-31")]["covered"] == 1
        assert (str(monthly.id), "2026-08-01") not in days

    def test_a_window_over_a_year_is_rejected(self, client: TestClient):
        response = client.get("/overview/coverage", params={"since": "2025-01-01", "until": "2026-08-13"})

        assert response.status_code == 422
```

Add `from collections import defaultdict` to the test imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-api/tests/routes/test_overview.py -k Coverage -v`
Expected: 404s (route missing) or assertion errors.

- [ ] **Step 3: Implement**

Add to `routes/overview.py` (imports: `from fastapi import HTTPException`, `from interloper.partitioning.time import TimeGranularity, TimePartition`, `from interloper_db.store.events import CoverageRow`):

```python
MAX_COVERAGE_DAYS = 366


class CoverageDay(BaseModel):
    """One job's asset-partitions on one day: expected, covered, failed."""

    date: dt.date
    job_id: UUID
    expected: int
    covered: int
    failed: int
    failed_run_id: UUID | None = None


class CoverageJob(BaseModel):
    """A job that has coverage in the window."""

    id: UUID
    name: str


class CoverageResponse(BaseModel):
    """The coverage calendar's data over a window of days."""

    since: dt.date
    until: dt.date
    jobs: list[CoverageJob]
    days: list[CoverageDay]


def coverage_days(
    rows: list[CoverageRow],
    jobs: list[Component],
    since: dt.date,
    until: dt.date,
    today: dt.date,
) -> list[CoverageDay]:
    """Roll partition rows onto days, per job, applying the calendar's day rules.

    An hourly key counts toward its day (24 slots per asset), a monthly or
    yearly key toward every day it spans. A job's days run from its first
    attempted day in the window to yesterday, or to its last attempted day
    when that is later (so today counts once attempted); a disabled job
    stops at its last attempted day. Days in that range with nothing
    attempted are expected and uncovered.

    Args:
        rows: The store's coverage rows for the window.
        jobs: The organisation's job rows (for ``enabled`` and ordering).
        since: First day of the window.
        until: Last day of the window, inclusive.
        today: The current UTC date, which decides what "yesterday" is.

    Returns:
        One entry per job and day with anything expected, by job then day.
    """
    by_job: dict[UUID, list[CoverageRow]] = defaultdict(list)
    for row in rows:
        by_job[row.job_id].append(row)
    enabled_by_id = {job.id: enabled(job) for job in jobs}

    result: list[CoverageDay] = []
    for job_id, job_rows in by_job.items():
        assets = {row.asset_id for row in job_rows}
        granularity = TimePartition.from_key(job_rows[0].partition_key).granularity
        slots = 24 if granularity is TimeGranularity.HOUR else 1
        attempted: dict[dt.date, set[tuple[UUID, str]]] = defaultdict(set)
        covered: dict[dt.date, set[tuple[UUID, str]]] = defaultdict(set)
        failed_run: dict[dt.date, UUID] = {}
        for row in job_rows:
            for day in _days_of(row.partition_key, since, until):
                attempted[day].add((row.asset_id, row.partition_key))
                if row.succeeded:
                    covered[day].add((row.asset_id, row.partition_key))
                elif row.failed_run_id is not None:
                    failed_run.setdefault(day, row.failed_run_id)
        first, last = min(attempted), max(attempted)
        end = last if not enabled_by_id.get(job_id, True) else max(last, min(today - dt.timedelta(days=1), until))
        day = first
        while day <= end:
            result.append(
                CoverageDay(
                    date=day,
                    job_id=job_id,
                    expected=len(assets) * slots,
                    covered=len(covered[day]),
                    failed=len(attempted[day] - covered[day]),
                    failed_run_id=failed_run.get(day),
                )
            )
            day += dt.timedelta(days=1)
    return result


def _days_of(key: str, since: dt.date, until: dt.date) -> list[dt.date]:
    """The days a partition key covers, clipped to the window.

    Args:
        key: A partition key of any granularity.
        since: First day of the window.
        until: Last day of the window, inclusive.

    Returns:
        The covered days inside the window, in order.
    """
    start, end = TimePartition.from_key(key).bounds
    first = start.date() if isinstance(start, dt.datetime) else start
    last = (end - dt.timedelta(microseconds=1)).date() if isinstance(end, dt.datetime) else end - dt.timedelta(days=1)
    day = max(first, since)
    days = []
    while day <= min(last, until):
        days.append(day)
        day += dt.timedelta(days=1)
    return days


@router.get("/coverage")
def get_coverage(
    user: ViewerDep,
    org_id: OrgIdDep,
    store: StoreDep,
    since: dt.date,
    until: dt.date,
    now: dt.datetime | None = Query(default=None, description="Reference instant; defaults to the current time"),
) -> CoverageResponse:
    """Per job and day coverage over a window of at most a year.

    Args:
        user: The authenticated user, required to hold at least the ``viewer`` role.
        org_id: The active organisation's UUID.
        store: The Store instance.
        since: First day of the window.
        until: Last day of the window, inclusive.
        now: Reference instant, overridable so the read is reproducible.

    Returns:
        The coverage calendar's data.

    Raises:
        HTTPException: 422 when the window is empty or longer than a year.
    """
    if until < since or (until - since).days >= MAX_COVERAGE_DAYS:
        raise HTTPException(status_code=422, detail=f"The window must span 1 to {MAX_COVERAGE_DAYS} days")
    today = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).date()
    jobs = store.components.list_all(org_id, kinds=["job"])
    rows = store.events.coverage_rows(org_id, since, until)
    days = coverage_days(rows, jobs, since, until, today)
    seen = {day.job_id for day in days}
    return CoverageResponse(
        since=since,
        until=until,
        jobs=[CoverageJob(id=job.id, name=job.name or job.key) for job in jobs if job.id in seen],
        days=days,
    )
```

Note on `_days_of`: `TimePartition.bounds` returns `date` for DAY/MONTH/YEAR and `datetime` for HOUR; the isinstance checks handle both. For an hourly key `2026-08-12T01`, bounds are `(2026-08-12 01:00, 2026-08-12 02:00)` so the covered day is `2026-08-12`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-api/tests/routes/test_overview.py -v`
Expected: PASS (all classes)

- [ ] **Step 5: Lint, type check, and update the spec's failing-job definition**

Run: `uv run ruff check packages/interloper-api && uv run ty check packages/interloper-api`

In `docs/superpowers/specs/2026-09-30-overview-page-design.md`, replace the **Failing job** definition with: "an enabled job whose latest stack (the newest run targeting it, read by its latest attempt) ended `failed`." Manual and scheduled backfills are indistinguishable in the data, so the "scheduler-fired only" qualifier is dropped.

---

### Task 6: Frontend types, store, page shell and navigation

**Files:**
- Create: `packages/interloper-app/app/app/types/overview.ts`
- Create: `packages/interloper-app/app/app/stores/overview.ts`
- Create: `packages/interloper-app/app/app/components/overview/Section.vue`
- Modify: `packages/interloper-app/app/app/pages/index.vue` (replace the redirect)
- Modify: `packages/interloper-app/app/app/composables/navigation.ts:73` (add Overview first)
- Modify: `packages/interloper-app/app/app/utils/time.ts` (add `relativeTime`)

**Interfaces:**
- Produces: `useOverviewStore()` with `overview: Ref<Overview | null>`, `coverage: Ref<Coverage | null>`, `coverageMonths: Ref<3 | 6 | 12>`, `loading`, `coverageLoading`, `fetchOverview()`, `fetchCoverage()`, `setCoverageMonths(m)`, `$reset()`; `OverviewSection` component with props `title`, `meta?`, `linkLabel?`, `linkTo?` and a `#actions` slot; `relativeTime(date: Date, now?: Date): string` returning `"in 4h"`, `"2h ago"`, `"tomorrow"`.

- [ ] **Step 1: Types**

Create `app/types/overview.ts`:

```ts
import type { Run } from '~/types/run'

export interface HourBucket {
    hour: string
    succeeded: number
    failed: number
}

export interface AttentionItem {
    kind: 'error_group' | 'connection' | 'run_stack' | 'drift' | 'overdue'
    severity: 'error' | 'warning'
    title: string
    target: string | null
    since: string | null
    component_id: string | null
    component_kind: string | null
    run_id: string | null
}

export interface UpcomingRun {
    job_id: string
    job_name: string
    next_run_at: string
    start_key: string | null
    end_key: string | null
}

export interface KindInventory {
    kind: string
    total: number
    healthy: number
    failing: number
    attention: number
    disabled: number
}

export interface Overview {
    generated_at: string
    runs: { total: number, succeeded: number, failed: number, hourly: HourBucket[] }
    activity: { running: number, queued: number, longest_running_seconds: number | null }
    backfills: { active: number, partitions_done: number, partitions_total: number }
    jobs: { enabled: number, failing: number }
    attention: AttentionItem[]
    upcoming: UpcomingRun[]
    recent: Run[]
    components: KindInventory[]
}

export interface CoverageDay {
    date: string
    job_id: string
    expected: number
    covered: number
    failed: number
    failed_run_id: string | null
}

export interface Coverage {
    since: string
    until: string
    jobs: { id: string, name: string }[]
    days: CoverageDay[]
}

export type CoverageMonths = 3 | 6 | 12
```

- [ ] **Step 2: Store**

Create `app/stores/overview.ts`:

```ts
import type { Coverage, CoverageMonths, Overview } from '~/types/overview'

/** One refetch per burst of realtime events, the way the executions store paces its dots. */
const REFRESH_DEBOUNCE = 1500

/** ISO date (YYYY-MM-DD) of a UTC instant. */
function isoDate(date: Date): string {
    return date.toISOString().slice(0, 10)
}

export const useOverviewStore = defineStore('overview', () => {
    const { apiFetch } = useApi()
    const orgStore = useOrganisationStore()

    const overview = ref<Overview | null>(null)
    const coverage = ref<Coverage | null>(null)
    const coverageMonths = ref<CoverageMonths>(6)
    const loading = ref(false)
    const coverageLoading = ref(false)
    const error = ref<Error | null>(null)

    async function fetchOverview() {
        loading.value = true
        error.value = null
        try {
            overview.value = await apiFetch<Overview>('/overview')
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function fetchCoverage() {
        coverageLoading.value = true
        try {
            const until = new Date()
            const since = new Date(until)
            since.setUTCMonth(since.getUTCMonth() - coverageMonths.value)
            const params = new URLSearchParams({ since: isoDate(since), until: isoDate(until) })
            coverage.value = await apiFetch<Coverage>(`/overview/coverage?${params}`)
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            coverageLoading.value = false
        }
    }

    async function setCoverageMonths(months: CoverageMonths) {
        coverageMonths.value = months
        await fetchCoverage()
    }

    let refreshTimer: ReturnType<typeof setTimeout> | undefined
    function scheduleRefresh() {
        clearTimeout(refreshTimer)
        refreshTimer = setTimeout(() => {
            fetchOverview()
            fetchCoverage()
        }, REFRESH_DEBOUNCE)
    }
    for (const table of ['runs', 'backfills', 'events'] as const) {
        useRealtimeSubscription({
            table,
            scope: () => overview.value ? orgStore.organisation?.id : null,
            onInsert: scheduleRefresh,
            onUpdate: scheduleRefresh,
            onDelete: scheduleRefresh,
        })
    }

    function $reset() {
        overview.value = null
        coverage.value = null
        loading.value = false
        coverageLoading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => {
        fetchOverview()
        fetchCoverage()
    }, $reset)

    return {
        overview,
        coverage,
        coverageMonths,
        loading,
        coverageLoading,
        error,
        fetchOverview,
        fetchCoverage,
        setCoverageMonths,
        $reset,
    }
})
```

Check `useRealtimeSubscription`'s options type in `app/composables/realtime.ts` (`table`, `scope`, `onInsert`, `onUpdate`, `onDelete`, optional `shouldHandle`); adjust the `as const` loop if `table` is typed as a union.

- [ ] **Step 3: Section shell and relative time**

Create `app/components/overview/Section.vue`:

```vue
<script setup lang="ts">
/** Design section: h2 + muted meta on the left, actions / accent link on the right, content below. */
defineProps<{
    title: string
    meta?: string
    linkLabel?: string
    linkTo?: string
}>()
</script>

<template>
    <section class="min-w-0">
        <div class="mb-3 flex flex-wrap items-center gap-3">
            <h2 class="text-[15px] font-semibold tracking-[-0.01em] text-highlighted whitespace-nowrap">{{ title }}</h2>
            <span v-if="meta"
                  class="min-w-0 truncate text-[12.5px] text-dimmed">{{ meta }}</span>
            <div class="ml-auto flex items-center gap-2.5">
                <slot name="actions" />
                <ULink v-if="linkLabel && linkTo"
                       :to="linkTo"
                       class="whitespace-nowrap text-[12.5px] font-medium text-primary">{{ linkLabel }}</ULink>
            </div>
        </div>
        <slot />
    </section>
</template>
```

Append to `app/utils/time.ts`:

```ts
/** Compact relative label: "in 4h", "2h ago", "tomorrow", "in 3d". */
export function relativeTime(date: Date, now: Date = new Date()): string {
    const diff = date.getTime() - now.getTime()
    const abs = Math.abs(diff)
    const future = diff >= 0
    if (abs < 60_000) return future ? 'now' : 'just now'
    const minutes = Math.round(abs / 60_000)
    const hours = Math.round(abs / 3_600_000)
    if (abs >= DAY && future && startOfDay(date.getTime()) === startOfDay(now.getTime() + DAY)) return 'tomorrow'
    const label = abs < 3_600_000 ? `${minutes}m` : abs < DAY ? `${hours}h` : `${Math.round(abs / DAY)}d`
    return future ? `in ${label}` : `${label} ago`
}
```

- [ ] **Step 4: Page shell and navigation**

Replace `app/pages/index.vue`:

```vue
<script setup lang="ts">
definePageMeta({ title: 'Overview' })

const overviewStore = useOverviewStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const { overview, loading } = storeToRefs(overviewStore)

onMounted(async () => {
    await Promise.all([
        overviewStore.fetchOverview(),
        overviewStore.fetchCoverage(),
        componentsStore.fetchAll(['job', 'source', 'asset', 'connection']),
        catalogStore.loaded ? Promise.resolve() : catalogStore.fetchCatalog(),
    ])
})
</script>

<template>
    <div class="flex max-w-[1360px] flex-col gap-7">
        <NavActions>
            <UButton icon="i-lucide-refresh-cw"
                     color="neutral"
                     variant="outline"
                     size="sm"
                     :loading="loading"
                     aria-label="Refresh"
                     @click="overviewStore.fetchOverview(); overviewStore.fetchCoverage()" />
        </NavActions>

        <OverviewHealthStrip v-if="overview"
                             :overview="overview" />
        <OverviewAttentionList v-if="overview"
                               :items="overview.attention"
                               :generated-at="overview.generated_at" />
        <OverviewTimelineSection :upcoming="overview?.upcoming ?? []" />
        <OverviewCoverageCalendar />
        <div v-if="overview"
             class="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <OverviewUpcomingList :items="overview.upcoming" />
            <OverviewRecentList :runs="overview.recent" />
        </div>
        <OverviewComponentsInventory v-if="overview"
                                     :rows="overview.components" />
    </div>
</template>
```

In `composables/navigation.ts`, add as the first destination of `useNavDestinations`:

```ts
        { label: 'Overview', icon: 'i-lucide-layout-dashboard', to: '/', keywords: ['home', 'dashboard', 'health'] },
```

`isNavActive` matches `path === '/'` exactly, and `path.startsWith('//')` never matches, so Overview is not active on every route. Verify by reading `isNavActive`; if `startsWith(\`${page.to}/\`)` with `to === '/'` matches everything, special-case: `page.to === '/' ? path === '/' : ...`.

- [ ] **Step 5: Typecheck the shell with stub components**

The five section components do not exist yet; typecheck will fail on unknown components until Tasks 7 to 11 land. Create each as a minimal placeholder now so the app builds between tasks:

`components/overview/HealthStrip.vue`, `AttentionList.vue`, `TimelineSection.vue`, `CoverageCalendar.vue`, `UpcomingList.vue`, `RecentList.vue`, `ComponentsInventory.vue`, each:

```vue
<script setup lang="ts">
defineProps<Record<string, unknown>>()
</script>

<template>
    <div />
</template>
```

Run from `packages/interloper-app/app/`: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

---

### Task 7: Health strip

Four tiles as in the design: runs (hourly sparkline), running now (running/queued bar, longest), backfills (progress), jobs failing (failing/healthy bar).

**Files:**
- Modify: `packages/interloper-app/app/app/components/overview/HealthStrip.vue`
- Create: `packages/interloper-app/app/app/components/overview/HealthTile.vue`

**Interfaces:**
- Consumes: `Overview` type (Task 6).
- `OverviewHealthTile` props: `label: string`, `to: string`, `headline: string | number`, `headlineClass?: string`; slots `#detail` (inline next to the headline) and `#footer`.

- [ ] **Step 1: Tile**

Create `components/overview/HealthTile.vue`:

```vue
<script setup lang="ts">
/** Design health tile: uppercase label, big tabular number with inline detail, footer visual pinned to the bottom. */
defineProps<{
    label: string
    to: string
    headline: string | number
    headlineClass?: string
}>()
</script>

<template>
    <NuxtLink :to="to"
              class="flex min-h-[132px] min-w-0 flex-col gap-3 rounded-lg border border-default bg-muted px-[18px] py-4 text-highlighted transition-colors hover:border-accented hover:bg-elevated">
        <div class="text-xs font-medium uppercase tracking-[.04em] text-dimmed">{{ label }}</div>
        <div class="flex items-baseline gap-2">
            <span class="text-[28px] font-semibold tracking-[-.02em] tabular-nums"
                  :class="headlineClass">{{ headline }}</span>
            <span class="text-[12.5px] text-muted"><slot name="detail" /></span>
        </div>
        <div class="mt-auto flex flex-col gap-1.5">
            <slot name="footer" />
        </div>
    </NuxtLink>
</template>
```

- [ ] **Step 2: Strip**

Replace `components/overview/HealthStrip.vue`:

```vue
<script setup lang="ts">
import type { Overview } from '~/types/overview'

const props = defineProps<{ overview: Overview }>()

const SPARK_HEIGHT = 30

const spark = computed(() => {
    const max = Math.max(1, ...props.overview.runs.hourly.map(b => b.succeeded + b.failed))
    return props.overview.runs.hourly.map((bucket, i) => ({
        key: bucket.hour,
        title: `${23 - i}h ago · ${bucket.succeeded} ok · ${bucket.failed} failed`,
        ok: bucket.succeeded ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.succeeded / max)) : 1,
        fail: bucket.failed ? Math.max(2, Math.round(SPARK_HEIGHT * bucket.failed / max)) : 0,
    }))
})

const activity = computed(() => {
    const { running, queued, longest_running_seconds } = props.overview.activity
    const total = running + queued
    return {
        running,
        queued,
        runningPct: total ? (100 * running) / total : 0,
        queuedPct: total ? (100 * queued) / total : 0,
        longest: longest_running_seconds === null ? null : formatElapsed(new Date(0), new Date(longest_running_seconds * 1000)),
    }
})

const backfillPct = computed(() => {
    const { partitions_done, partitions_total } = props.overview.backfills
    return partitions_total ? Math.round((100 * partitions_done) / partitions_total) : 0
})

const jobs = computed(() => {
    const { enabled, failing } = props.overview.jobs
    return {
        failing,
        healthy: enabled - failing,
        failingPct: enabled ? (100 * failing) / enabled : 0,
        healthyPct: enabled ? (100 * (enabled - failing)) / enabled : 100,
    }
})
</script>

<template>
    <div class="grid grid-cols-2 gap-4 xl:grid-cols-4">
        <OverviewHealthTile label="Runs · last 24h"
                            to="/executions/runs"
                            :headline="overview.runs.total">
            <template #detail>
                {{ overview.runs.succeeded }} succeeded ·
                <span :class="overview.runs.failed ? 'font-medium text-error' : ''">{{ overview.runs.failed }} failed</span>
            </template>
            <template #footer>
                <div class="flex items-end gap-0.5"
                     :style="{ height: `${SPARK_HEIGHT}px` }">
                    <div v-for="bar in spark"
                         :key="bar.key"
                         class="flex h-full flex-1 flex-col justify-end gap-px"
                         :title="bar.title">
                        <div v-if="bar.fail"
                             class="rounded-t-sm bg-error"
                             :style="{ height: `${bar.fail}px` }" />
                        <div class="rounded-sm"
                             :class="bar.ok > 1 ? 'bg-success' : 'bg-accented'"
                             :style="{ height: `${bar.ok}px` }" />
                    </div>
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed"><span>24h ago</span><span>now</span></div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Running now"
                            to="/executions/runs"
                            :headline="activity.running">
            <template #detail>{{ activity.queued }} queued</template>
            <template #footer>
                <div class="flex h-1.5 gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-primary"
                         :style="{ width: `${activity.runningPct}%` }" />
                    <div class="bg-(--ui-text-dimmed)/40"
                         :style="{ width: `${activity.queuedPct}%` }" />
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed">
                    <span>{{ activity.running }} running · {{ activity.queued }} queued</span>
                    <span v-if="activity.longest">longest {{ activity.longest }}</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Backfills in progress"
                            to="/executions/backfills"
                            :headline="overview.backfills.active">
            <template #detail>{{ overview.backfills.partitions_done }} of {{ overview.backfills.partitions_total }} partitions</template>
            <template #footer>
                <div class="flex h-1.5 overflow-hidden rounded-full bg-accented">
                    <div class="bg-primary"
                         :style="{ width: `${backfillPct}%` }" />
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed">
                    <span>combined progress</span>
                    <span class="font-semibold text-primary">{{ backfillPct }}%</span>
                </div>
            </template>
        </OverviewHealthTile>

        <OverviewHealthTile label="Jobs failing"
                            :to="kindPath('job')"
                            :headline="jobs.failing"
                            :headline-class="jobs.failing ? 'text-error' : ''">
            <template #detail>of {{ overview.jobs.enabled }} enabled</template>
            <template #footer>
                <div class="flex h-1.5 gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div v-if="jobs.failing"
                         class="bg-error"
                         :style="{ width: `${jobs.failingPct}%` }" />
                    <div class="bg-success"
                         :style="{ width: `${jobs.healthyPct}%` }" />
                </div>
                <div class="flex justify-between text-[10.5px] text-dimmed">
                    <span>{{ jobs.failing ? `${jobs.failing} failing` : 'none failing' }}</span>
                    <span>{{ jobs.healthy }} healthy</span>
                </div>
            </template>
        </OverviewHealthTile>
    </div>
</template>
```

- [ ] **Step 3: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

---

### Task 8: Needs attention list, with the connection edit deep link

**Files:**
- Modify: `packages/interloper-app/app/app/components/overview/AttentionList.vue`
- Modify: `packages/interloper-app/app/app/pages/components/[kind].vue:49-60` (accept `?edit=<id>`)

**Interfaces:**
- Consumes: `AttentionItem[]`, `userStore.user.role`.
- Produces: `/components/connections?edit=<id>` opens the edit wizard for that connection.

- [ ] **Step 1: Deep link**

In `pages/components/[kind].vue`, extend the deep-link `watchEffect` so that `?edit=<id>` loads the component and opens the edit wizard, consuming the query like `?new` does:

```ts
watchEffect(() => {
    const key = route.query.new
    const edit = route.query.edit
    if (key === undefined && edit === undefined) return
    if (typeof edit === 'string' && edit) {
        componentsStore.fetchOne(edit).then(handleEdit).catch(() => {})
    }
    else {
        const name = typeof route.query.name === 'string' ? route.query.name : undefined
        if (typeof key === 'string' && key) handleCreateFromCatalog(key, name)
        else handleCreate()
    }
    router.replace({ query: { ...route.query, new: undefined, name: undefined, edit: undefined } })
})
```

Update the comment above it to mention `?edit=<id>` (the overview's Reconnect link).

- [ ] **Step 2: List**

Replace `components/overview/AttentionList.vue`:

```vue
<script setup lang="ts">
import type { AttentionItem } from '~/types/overview'

const props = defineProps<{
    items: AttentionItem[]
    generatedAt: string
}>()

const userStore = useUserStore()
const editor = computed(() => userStore.user?.role === 'editor' || userStore.user?.role === 'admin')

const KIND_META: Record<AttentionItem['kind'], { label: string, icon: string, rowIcon: string }> = {
    error_group: { label: 'Error group', icon: 'i-lucide-circle-alert', rowIcon: 'i-lucide-x' },
    connection: { label: 'Connection', icon: 'i-lucide-key-round', rowIcon: 'i-lucide-key-round' },
    run_stack: { label: 'Run stack', icon: 'i-lucide-activity', rowIcon: 'i-lucide-repeat' },
    drift: { label: 'Catalog drift', icon: 'i-lucide-library', rowIcon: 'i-lucide-circle-help' },
    overdue: { label: 'Overdue job', icon: 'i-lucide-calendar-clock', rowIcon: 'i-lucide-clock' },
}

function openTarget(item: AttentionItem): { label: string, to: string } {
    switch (item.kind) {
        case 'error_group':
        case 'run_stack':
            return { label: 'Open run', to: `/executions/runs/${item.run_id}` }
        case 'connection':
            return { label: 'Open connection', to: kindPath('connection') }
        case 'drift':
            return { label: 'Open collection', to: '/collection' }
        case 'overdue':
            return { label: 'Open job', to: kindPath('job') }
    }
}

function fix(item: AttentionItem): { label: string, icon: string, to: string } | null {
    if (!editor.value) return null
    if (item.kind === 'connection' && item.component_id) {
        return { label: 'Reconnect', icon: 'i-lucide-plug-zap', to: `${kindPath('connection')}?edit=${item.component_id}` }
    }
    return null
}

function when(item: AttentionItem): string {
    if (!item.since) return ''
    if (item.kind === 'overdue') return `due ${formatDate(item.since)}`
    if (item.kind === 'error_group') return 'last 24h'
    return `${timeSince(new Date(item.since))} ago`
}
</script>

<template>
    <OverviewSection title="Needs attention"
                     :meta="items.length ? `${items.length} item${items.length === 1 ? '' : 's'}` : undefined">
        <div v-if="items.length"
             class="overflow-hidden rounded-lg border border-default divide-y divide-default">
            <div v-for="item in items"
                 :key="`${item.kind}:${item.component_id ?? item.run_id ?? item.title}`"
                 class="flex items-center gap-3.5 px-4 py-3 transition-colors hover:bg-muted">
                <span class="inline-flex size-[30px] shrink-0 items-center justify-center rounded-full"
                      :class="item.severity === 'error' ? 'bg-error/10 text-error' : 'bg-warning/15 text-warning'">
                    <UIcon :name="KIND_META[item.kind].rowIcon"
                           class="size-[15px]" />
                </span>
                <div class="flex min-w-0 flex-1 flex-col gap-0.5">
                    <div class="text-[13.5px] font-medium leading-snug text-highlighted">{{ item.title }}</div>
                    <div class="flex flex-wrap items-center gap-2 text-xs text-dimmed">
                        <span class="inline-flex items-center gap-1.5 text-muted">
                            <UIcon :name="KIND_META[item.kind].icon"
                                   class="size-[13px]" />{{ KIND_META[item.kind].label }}
                        </span>
                        <template v-if="item.target">
                            <span>·</span>
                            <span class="font-mono text-[11.5px]">{{ item.target }}</span>
                        </template>
                        <template v-if="when(item)">
                            <span>·</span>
                            <span>{{ when(item) }}</span>
                        </template>
                    </div>
                </div>
                <div class="flex shrink-0 items-center gap-2">
                    <UButton v-if="fix(item)"
                             :icon="fix(item)!.icon"
                             :label="fix(item)!.label"
                             :to="fix(item)!.to"
                             size="sm" />
                    <UButton :label="openTarget(item).label"
                             :to="openTarget(item).to"
                             size="sm"
                             color="neutral"
                             variant="outline" />
                </div>
            </div>
        </div>
        <div v-else
             class="flex items-center gap-3.5 rounded-lg border border-default bg-muted px-5 py-[22px]">
            <span class="inline-flex size-[34px] shrink-0 items-center justify-center rounded-full bg-success/10 text-success">
                <UIcon name="i-lucide-check"
                       class="size-4" />
            </span>
            <div class="flex flex-col gap-0.5">
                <div class="text-sm font-semibold text-highlighted">All clear</div>
                <div class="text-[13px] text-muted">No failures, drift or overdue jobs. Last checked {{ formatClockTime(new Date(generatedAt)) }}.</div>
            </div>
        </div>
    </OverviewSection>
</template>
```

- [ ] **Step 3: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`

---

### Task 9: Timeline with scheduled ghosts

Embed `ChartExecutionTimeline` with a window that extends past now, a hatched future region, a "Now" marker, and one dashed bar per enabled job at its `next_run_at`.

**Files:**
- Modify: `packages/interloper-app/app/app/types/timeline.ts` (widen `TimelineBar.status`)
- Modify: `packages/interloper-app/app/app/components/chart/ExecutionTimeline.vue` (`futureFrom` prop, scheduled bar style, future region)
- Modify: `packages/interloper-app/app/app/stores/timeline.ts` (window split: `futureRatio`)
- Modify: `packages/interloper-app/app/app/components/overview/TimelineSection.vue`

**Interfaces:**
- `TimelineBar.status: ExecutionStatus | 'scheduled'`.
- `ChartExecutionTimeline` new prop `futureFrom?: number | null` (epoch ms): the plot from this instant to the window's end is hatched; bars with status `scheduled` render as a dashed outline.
- `useTimelineStore.setFutureRatio(ratio: number)`: fraction of the span placed after now (0 by default; the overview sets 1/3). `rangeEnd` becomes `now + span * ratio`, `rangeStart = rangeEnd - span`.

- [ ] **Step 1: Types and store**

In `types/timeline.ts`: `status: ExecutionStatus | 'scheduled'` on `TimelineBar`, with the doc comment "`scheduled` is a firing that has not happened yet: drawn as an outline."

In `stores/timeline.ts`:
- add `const futureRatio = ref(0)`;
- in `fetch()`: `rangeEnd.value = Date.now() + span.value * futureRatio.value; rangeStart.value = rangeEnd.value - span.value;` and request runs with `before: new Date(Math.min(rangeEnd.value, Date.now())).toISOString()`;
- add `function setFutureRatio(ratio: number) { futureRatio.value = ratio }` and export it along with `futureRatio`;
- `$reset` sets `futureRatio.value = 0`.

- [ ] **Step 2: Chart**

In `components/chart/ExecutionTimeline.vue`:

Props: add
```ts
    /** Epoch ms from which the plot is the future: hatched, and scheduled bars live there. */
    futureFrom?: number | null
```
with default `null`.

After `markerPercent`, add:
```ts
const futurePercent = computed(() => {
    if (props.futureFrom === null) return null
    const pct = toPercent(props.futureFrom - baseTime.value)
    return pct < 0 ? 0 : pct > 100 ? null : pct
})
```

`getStatusColor` for `'scheduled'` returns `'transparent'` (add a guard before the lookup).

In the template, inside the plot area before `<!-- Gridlines -->`:
```vue
                <div v-if="futurePercent !== null"
                     class="pointer-events-none absolute top-0 bottom-0 bg-[repeating-linear-gradient(135deg,var(--ui-bg-muted)_0_6px,var(--ui-bg-elevated)_6px_7px)]"
                     :style="{ left: `${futurePercent}%`, right: 0 }" />
```

On the time bar `div`, add `:class` entries so scheduled bars draw as a dashed outline:
```vue
                         :class="[
                             labelWidth ? '' : 'px-2',
                             layout.bar.status === 'scheduled' ? 'border-[1.5px] border-dashed border-dimmed bg-default' : '',
                         ]"
```
and keep `backgroundColor: getStatusColor(layout.bar.status)` (transparent for scheduled, so the class background shows).

The marker: give it a label when `futureFrom` is set. Replace the marker `div` with:
```vue
                <div v-if="markerPercent !== null"
                     class="pointer-events-none absolute top-0 bottom-0 z-10 w-0.5 bg-primary/60"
                     :style="{ left: `${markerPercent}%` }">
                    <span v-if="futureFrom !== null"
                          class="absolute top-1.5 -translate-x-1/2 whitespace-nowrap rounded-md bg-primary px-2 py-0.5 text-[11px] font-semibold tabular-nums text-white">Now · {{ formatClockTime(new Date(markerTime!)) }}</span>
                </div>
```

The `now` ref only ticks while something runs (`hasRunning`). The marker uses `props.markerTime`, which the section refreshes on its own interval, so nothing else changes.

- [ ] **Step 3: Section**

Replace `components/overview/TimelineSection.vue`:

```vue
<script setup lang="ts">
import type { UpcomingRun } from '~/types/overview'
import type { TimelineBar, TimelineRow } from '~/types/timeline'

const props = defineProps<{ upcoming: UpcomingRun[] }>()

const LABEL_WIDTH = 250
const ROW_HEIGHT = 40
const AXIS_HEIGHT = 30
const MAX_ROWS = 10
const REFRESH_INTERVAL = 60_000
/** The window's share that lies ahead of now, so scheduled firings appear as ghosts. */
const FUTURE_RATIO = 1 / 3

const timelineStore = useTimelineStore()
const userStore = useUserStore()
const { runs, span, rangeStart, rangeEnd, loading } = storeToRefs(timelineStore)

const runRows = useRunTimelineRows(runs)
const now = ref(new Date())

/** Job rows gain a dashed bar at their next firing; its length is the job's latest completed run in view. */
const rows = computed<TimelineRow[]>(() => runRows.value.map((row) => {
    const slot = props.upcoming.find(u => u.job_id === row.id)
    if (!slot) return row
    const start = new Date(slot.next_run_at).getTime()
    if (start < now.value.getTime() || start > rangeEnd.value) return row
    const durations = row.bars.filter(b => b.end !== null).map(b => b.end! - b.start)
    const ghost: TimelineBar = {
        id: `scheduled:${row.id}`,
        status: 'scheduled',
        start,
        end: start + (durations.length ? durations[durations.length - 1]! : 0),
        detail: slot.start_key ? (slot.start_key === slot.end_key ? slot.start_key : `${slot.start_key} → ${slot.end_key}`) : undefined,
    }
    return { ...row, bars: [...row.bars, ghost] }
}))

const spanItems = TIMELINE_SPANS.map(s => ({ label: s.label, value: String(s.value) }))
const activeSpan = computed({
    get: () => String(span.value),
    set: (value: string) => timelineStore.setSpan(Number(value)),
})

const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)
const rangeLabel = computed(() => `${formatDate(new Date(rangeStart.value))} → ${formatDate(new Date(rangeEnd.value))}`)
const height = computed(() => AXIS_HEIGHT + Math.min(rows.value.length, MAX_ROWS) * ROW_HEIGHT + 1)

function onBarClick(bar: TimelineBar, row: TimelineRow) {
    if (bar.status === 'scheduled') navigateTo(kindPath('job'))
    else navigateTo(`/executions/runs/${bar.id}`)
}

let refreshTimer: ReturnType<typeof setInterval> | null = null
onMounted(async () => {
    timelineStore.setFutureRatio(FUTURE_RATIO)
    await timelineStore.fetch()
    refreshTimer = setInterval(() => {
        now.value = new Date()
        timelineStore.fetch()
    }, REFRESH_INTERVAL)
})
onUnmounted(() => {
    if (refreshTimer) clearInterval(refreshTimer)
    timelineStore.$reset()
})
</script>

<template>
    <OverviewSection title="Timeline"
                     :meta="`${timezone} · ${rangeLabel}`"
                     link-label="All executions"
                     link-to="/executions/runs">
        <template #actions>
            <span class="text-xs text-dimmed">Window</span>
            <UTabs v-model="activeSpan"
                   :items="spanItems"
                   variant="pill"
                   size="xs"
                   :content="false" />
        </template>
        <div class="overflow-hidden rounded-lg border border-default"
             :style="{ height: `${height}px` }">
            <ChartExecutionTimeline :rows="rows"
                                    :range-start="rangeStart"
                                    :range-end="rangeEnd"
                                    :marker-time="now"
                                    :future-from="now.getTime()"
                                    axis="clock"
                                    :label-width="LABEL_WIDTH"
                                    label-title="Target"
                                    :empty-message="loading ? 'Loading…' : 'No runs in this window'"
                                    @bar-click="onBarClick" />
        </div>
        <div class="mt-2.5 flex items-center gap-4 text-[11.5px] text-dimmed">
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-success" />Success</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-error" />Failed</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-primary" />Running</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] bg-accented" />Queued</span>
            <span class="inline-flex items-center gap-1.5"><span class="h-2 w-3.5 rounded-[3px] border-[1.5px] border-dashed border-dimmed" />Scheduled</span>
        </div>
    </OverviewSection>
</template>
```

- [ ] **Step 4: Regression check on `/timeline`**

The timeline page (`pages/timeline.vue`) must render exactly as before: `futureRatio` defaults to 0 and it passes no `futureFrom`. Open both pages in the dev instance (Task 13) and compare.

- [ ] **Step 5: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`

---

### Task 10: Coverage calendar and day detail

ECharts `calendar` + `heatmap`, coloured per the design, with a job filter, 3/6/12 month window, a summary line, and a day detail panel under it. Backfill from the panel opens `ExecutionsRunModal` preset to that job and day.

**Files:**
- Create: `packages/interloper-app/app/app/composables/coverage.ts`
- Modify: `packages/interloper-app/app/app/components/overview/CoverageCalendar.vue`
- Create: `packages/interloper-app/app/app/components/overview/CoverageDayDetail.vue`
- Modify: `packages/interloper-app/app/app/components/executions/RunModal.vue` (`initialRange` prop)

**Interfaces:**
- `useCoverageCalendar(coverage: MaybeRefOrGetter<Coverage | null>, jobFilter: MaybeRefOrGetter<string>)` returns `{ byDate: ComputedRef<Map<string, DayAggregate>>, summary: ComputedRef<string> }` where `DayAggregate = { expected: number, covered: number, failed: number }`.
- `cellColor(aggregate: DayAggregate | undefined, dark: boolean): string` exported from the same composable.
- `ExecutionsRunModal` prop `initialRange?: { start: string, end: string }` (partition keys); when set, the modal opens on that range instead of today.

- [ ] **Step 1: Composable**

Create `composables/coverage.ts`:

```ts
import type { MaybeRefOrGetter } from 'vue'
import type { Coverage } from '~/types/overview'

export interface DayAggregate {
    expected: number
    covered: number
    failed: number
}

/** Design cell colours; the amber tints are the partial states. */
const CELL = {
    covered: { light: '#1fa463', dark: '#45bc84' },
    partial: { light: '#f2b23e', dark: '#e9ac46' },
    partialLow: { light: '#f9dca0', dark: '#8a6a2a' },
    failed: { light: '#e5484d', dark: '#ea686c' },
    failedLow: { light: '#f4a5a8', dark: '#a04448' },
    empty: { light: '#f4f4f5', dark: '#27272a' },
}

/** The colour a day's cell takes, from what its jobs delivered. */
export function cellColor(day: DayAggregate | undefined, dark: boolean): string {
    const mode = dark ? 'dark' : 'light'
    if (!day) return CELL.empty[mode]
    if (day.failed > 0) return day.failed / day.expected >= 0.5 ? CELL.failed[mode] : CELL.failedLow[mode]
    const ratio = day.covered / day.expected
    if (ratio >= 1) return CELL.covered[mode]
    return ratio >= 0.6 ? CELL.partial[mode] : CELL.partialLow[mode]
}

/** Sum the coverage rows of the selected jobs per date, and phrase the window's summary. */
export function useCoverageCalendar(
    coverage: MaybeRefOrGetter<Coverage | null>,
    jobFilter: MaybeRefOrGetter<string>,
) {
    const byDate = computed(() => {
        const map = new Map<string, DayAggregate>()
        const filter = toValue(jobFilter)
        for (const day of toValue(coverage)?.days ?? []) {
            if (filter !== 'all' && day.job_id !== filter) continue
            const agg = map.get(day.date) ?? { expected: 0, covered: 0, failed: 0 }
            agg.expected += day.expected
            agg.covered += day.covered
            agg.failed += day.failed
            map.set(day.date, agg)
        }
        return map
    })

    const summary = computed(() => {
        let expected = 0
        let covered = 0
        let gaps = 0
        let failures = 0
        for (const day of byDate.value.values()) {
            expected += day.expected
            covered += day.covered
            if (day.failed) failures++
            else if (day.covered < day.expected) gaps++
        }
        if (!expected) return 'Nothing expected in this window'
        const pct = Math.round((100 * covered) / expected)
        return `${pct}% of ${expected.toLocaleString()} partitions · ${gaps} days with gaps · ${failures} with failures`
    })

    return { byDate, summary }
}
```

- [ ] **Step 2: RunModal preset**

In `components/executions/RunModal.vue`, add to the props:
```ts
    /** Partition keys to open on instead of today (e.g. the coverage calendar's selected day). */
    initialRange?: { start: string, end: string }
```
(default `undefined`), import `parseDate` from `@internationalized/date`, and in the `watch(open, ...)` handler replace the three reset lines with:
```ts
        const t = today(clockZone.value)
        const preset = props.initialRange
        dateRange.value = preset && granularity.value === 'day'
            ? { start: parseDate(preset.start), end: parseDate(preset.end) }
            : { start: t, end: t }
        startKey.value = preset?.start ?? previousPeriodKey(granularity.value, clockZone.value)
        endKey.value = preset?.end ?? startKey.value
```

- [ ] **Step 3: Day detail**

Create `components/overview/CoverageDayDetail.vue`:

```vue
<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'
import type { Coverage, CoverageDay } from '~/types/overview'

const props = defineProps<{
    date: string
    coverage: Coverage
    jobFilter: string
}>()

const userStore = useUserStore()
const componentsStore = useComponentsStore()
const editor = computed(() => userStore.user?.role === 'editor' || userStore.user?.role === 'admin')

const rows = computed(() => props.coverage.days
    .filter(d => d.date === props.date && (props.jobFilter === 'all' || d.job_id === props.jobFilter))
    .map(d => ({
        ...d,
        name: props.coverage.jobs.find(j => j.id === d.job_id)?.name ?? d.job_id.slice(0, 8),
        okPct: Math.round((100 * d.covered) / d.expected),
        failPct: Math.round((100 * d.failed) / d.expected),
        gap: d.covered + d.failed < d.expected,
    })))

const summary = computed(() => {
    const expected = rows.value.reduce((n, r) => n + r.expected, 0)
    const covered = rows.value.reduce((n, r) => n + r.covered, 0)
    const failed = rows.value.reduce((n, r) => n + r.failed, 0)
    if (!rows.value.length) return 'Nothing expected on this day'
    const parts = [`${covered} of ${expected} partitions covered`]
    if (failed) parts.push(`${failed} failed`)
    if (covered + failed < expected) parts.push(`${expected - covered - failed} missing`)
    return parts.join(' · ')
})

const label = computed(() => new Date(`${props.date}T00:00:00Z`).toLocaleDateString(undefined, {
    weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
}))

const backfillJob = ref<ComponentRecord | null>(null)
const backfillOpen = ref(false)
function backfill(row: CoverageDay) {
    const job = componentsStore.byId(row.job_id)
    if (!job) return
    backfillJob.value = job
    backfillOpen.value = true
}
</script>

<template>
    <div class="border-t border-default bg-muted px-5 pb-4 pt-3.5">
        <div class="mb-2.5 flex flex-wrap items-baseline gap-2.5">
            <span class="text-[13.5px] font-semibold text-highlighted">{{ label }}</span>
            <span class="text-[12.5px] text-muted">{{ summary }}</span>
        </div>
        <div v-if="rows.length"
             class="grid grid-cols-[minmax(160px,220px)_minmax(0,1fr)_72px_auto] items-center gap-x-4 gap-y-2">
            <template v-for="row in rows"
                      :key="row.job_id">
                <span class="truncate font-mono text-xs text-highlighted">{{ row.name }}</span>
                <div class="flex h-2 overflow-hidden rounded-full bg-accented">
                    <div class="bg-success"
                         :style="{ width: `${row.okPct}%` }" />
                    <div class="bg-error"
                         :style="{ width: `${row.failPct}%` }" />
                </div>
                <span class="whitespace-nowrap text-xs tabular-nums text-muted">{{ row.covered }} / {{ row.expected }}</span>
                <div class="flex min-w-[92px] justify-end">
                    <UButton v-if="editor && row.gap && !row.failed"
                             icon="i-lucide-history"
                             label="Backfill"
                             size="xs"
                             color="neutral"
                             variant="outline"
                             @click="backfill(row)" />
                    <ULink v-else-if="row.failed && row.failed_run_id"
                           :to="`/executions/runs/${row.failed_run_id}`"
                           class="inline-flex items-center gap-1 text-xs text-error hover:underline">Open run<UIcon name="i-lucide-arrow-right"
                                                                                                                       class="size-3" /></ULink>
                    <span v-else-if="!row.gap"
                          class="inline-flex items-center gap-1 text-xs text-success"><UIcon name="i-lucide-check"
                                                                                               class="size-3" />Complete</span>
                </div>
            </template>
        </div>
        <ExecutionsRunModal v-if="backfillJob"
                            v-model:open="backfillOpen"
                            :target="backfillJob"
                            :initial-range="{ start: date, end: date }" />
    </div>
</template>
```

- [ ] **Step 4: Calendar**

Replace `components/overview/CoverageCalendar.vue`:

```vue
<script setup lang="ts">
import VChart from 'vue-echarts'
import { HeatmapChart } from 'echarts/charts'
import { CalendarComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'
import type { CoverageMonths } from '~/types/overview'

use([CanvasRenderer, HeatmapChart, CalendarComponent, TooltipComponent])

const overviewStore = useOverviewStore()
const { coverage, coverageMonths, coverageLoading } = storeToRefs(overviewStore)
const colorMode = useColorMode()

const jobFilter = ref('all')
const selected = ref<string | null>(null)
const { byDate, summary } = useCoverageCalendar(coverage, jobFilter)

const jobOptions = computed(() => [
    { label: 'All jobs', value: 'all' },
    ...(coverage.value?.jobs ?? []).map(j => ({ label: j.name, value: j.id })),
])
const windowItems = ([3, 6, 12] as CoverageMonths[]).map(m => ({ label: `${m}m`, value: String(m) }))
const activeWindow = computed({
    get: () => String(coverageMonths.value),
    set: (value: string) => overviewStore.setCoverageMonths(Number(value) as CoverageMonths),
})

/** Cell geometry follows the window so 12 months still fit the card. */
const cell = computed(() => coverageMonths.value <= 3 ? 22 : coverageMonths.value <= 6 ? 15 : 11)
const gap = computed(() => coverageMonths.value <= 6 ? 3 : 2)

const option = computed(() => {
    if (!coverage.value) return {}
    const dark = colorMode.value === 'dark'
    const axis = dark ? CHART_AXIS_COLORS.axis.dark : CHART_AXIS_COLORS.axis.light
    const line = dark ? CHART_AXIS_COLORS.grid.dark : CHART_AXIS_COLORS.grid.light
    const surface = dark ? '#18181b' : '#ffffff'
    const data: unknown[] = []
    const cursor = new Date(`${coverage.value.since}T00:00:00Z`)
    const until = new Date(`${coverage.value.until}T00:00:00Z`)
    while (cursor <= until) {
        const date = cursor.toISOString().slice(0, 10)
        const day = byDate.value.get(date)
        data.push({
            value: [date, day ? day.covered / Math.max(1, day.expected) : 0],
            itemStyle: {
                color: cellColor(day, dark),
                borderColor: selected.value === date ? (dark ? '#fafafa' : '#09090b') : surface,
                borderWidth: selected.value === date ? 2 : gap.value / 2,
            },
        })
        cursor.setUTCDate(cursor.getUTCDate() + 1)
    }
    return {
        tooltip: {
            formatter: (params: any) => {
                const date = params.data.value[0]
                const day = byDate.value.get(date)
                if (!day) return `<b>${date}</b><br/>nothing expected`
                return `<b>${date}</b><br/>${day.covered}/${day.expected} covered${day.failed ? ` · ${day.failed} failed` : ''}`
            },
        },
        calendar: {
            left: 36,
            top: 28,
            right: 8,
            cellSize: [cell.value, cell.value],
            range: [coverage.value.since, coverage.value.until],
            orient: 'horizontal',
            splitLine: { show: false },
            itemStyle: { color: surface, borderWidth: 0 },
            dayLabel: { firstDay: 1, nameMap: ['', 'Mon', '', 'Wed', '', 'Fri', ''], color: axis, fontSize: 10.5, margin: 6 },
            monthLabel: { color: axis, fontSize: 11, margin: 8 },
            yearLabel: { show: false },
        },
        series: [{
            type: 'heatmap',
            coordinateSystem: 'calendar',
            data,
            emphasis: { itemStyle: { borderColor: line, borderWidth: 1 } },
        }],
    }
})

const chartHeight = computed(() => 28 + 7 * (cell.value + gap.value) + 12)

function onClick(params: any) {
    const date = params?.data?.value?.[0]
    if (typeof date === 'string' && byDate.value.has(date)) selected.value = date
}

watch(coverage, (value) => {
    if (!value || (selected.value && byDate.value.has(selected.value))) return
    // Land on the most recent day that has anything to say, so the panel is never empty on load.
    selected.value = [...byDate.value.keys()].sort().at(-1) ?? null
})
</script>

<template>
    <OverviewSection title="Partition coverage"
                     :meta="summary">
        <template #actions>
            <USelect v-model="jobFilter"
                     :items="jobOptions"
                     size="xs"
                     class="w-44" />
            <UTabs v-model="activeWindow"
                   :items="windowItems"
                   variant="pill"
                   size="xs"
                   :content="false" />
        </template>
        <div class="overflow-hidden rounded-lg border border-default">
            <div class="overflow-x-auto px-5 pb-4 pt-[18px]">
                <VChart v-if="coverage"
                        :option="option"
                        :style="{ height: `${chartHeight}px`, minWidth: '600px' }"
                        autoresize
                        @click="onClick" />
                <div v-else
                     class="flex h-[140px] items-center justify-center text-sm text-muted">
                    {{ coverageLoading ? 'Loading coverage…' : 'No coverage yet' }}
                </div>
                <div class="mt-3.5 flex items-center gap-4 text-[11.5px] text-dimmed">
                    <span class="inline-flex items-center gap-1.5"><span class="size-[11px] rounded-sm bg-success" />Covered</span>
                    <span class="inline-flex items-center gap-1.5"><span class="size-[11px] rounded-sm bg-warning" />Partially covered</span>
                    <span class="inline-flex items-center gap-1.5"><span class="size-[11px] rounded-sm bg-error" />Failed</span>
                    <span class="inline-flex items-center gap-1.5"><span class="size-[11px] rounded-sm bg-elevated ring-1 ring-inset ring-default" />Not expected</span>
                    <span class="ml-auto">Click a day to see it per job</span>
                </div>
            </div>
            <OverviewCoverageDayDetail v-if="coverage && selected"
                                       :date="selected"
                                       :coverage="coverage"
                                       :job-filter="jobFilter" />
        </div>
    </OverviewSection>
</template>
```

Dark surface `#18181b` is the app's dark `--ui-bg` (see `assets/css/main.css`); confirm and adjust if it differs.

- [ ] **Step 5: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`

---

### Task 11: Coming up, Just happened, Components inventory

**Files:**
- Modify: `packages/interloper-app/app/app/components/overview/UpcomingList.vue`
- Modify: `packages/interloper-app/app/app/components/overview/RecentList.vue`
- Modify: `packages/interloper-app/app/app/components/overview/ComponentsInventory.vue`

- [ ] **Step 1: Upcoming**

```vue
<script setup lang="ts">
import type { UpcomingRun } from '~/types/overview'

const props = defineProps<{ items: UpcomingRun[] }>()
const LIMIT = 4

const userStore = useUserStore()
const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)

const rows = computed(() => props.items.slice(0, LIMIT).map((item) => {
    const at = new Date(item.next_run_at)
    return {
        ...item,
        time: formatClockTime(at),
        partition: item.start_key ? (item.start_key === item.end_key ? item.start_key : `${item.start_key} → ${item.end_key}`) : '',
        rel: relativeTime(at),
    }
}))
</script>

<template>
    <OverviewSection title="Coming up"
                     :meta="timezone"
                     link-label="All jobs"
                     :link-to="kindPath('job')">
        <div class="overflow-hidden rounded-lg border border-default divide-y divide-default">
            <NuxtLink v-for="row in rows"
                      :key="row.job_id"
                      :to="kindPath('job')"
                      class="flex items-center gap-3 px-4 py-[11px] text-highlighted transition-colors hover:bg-muted">
                <span class="w-[58px] shrink-0 text-[13px] font-semibold tabular-nums">{{ row.time }}</span>
                <UIcon name="i-lucide-calendar-clock"
                       class="size-4 shrink-0 text-dimmed" />
                <span class="min-w-0 flex-1 truncate font-mono text-[12.5px]">{{ row.job_name }}</span>
                <span class="whitespace-nowrap text-xs text-dimmed">{{ row.partition }}</span>
                <span class="w-14 whitespace-nowrap text-right text-xs text-dimmed">{{ row.rel }}</span>
            </NuxtLink>
            <div v-if="!rows.length"
                 class="px-4 py-5 text-sm text-muted">Nothing scheduled.</div>
        </div>
    </OverviewSection>
</template>
```

- [ ] **Step 2: Recent**

```vue
<script setup lang="ts">
import type { Run } from '~/types/run'

defineProps<{ runs: Run[] }>()

const TONE: Record<string, { color: 'success' | 'error' | 'primary' | 'neutral' | 'warning', dot: string }> = {
    success: { color: 'success', dot: 'bg-success' },
    failed: { color: 'error', dot: 'bg-error' },
    running: { color: 'primary', dot: 'bg-primary' },
    canceled: { color: 'warning', dot: 'bg-warning' },
}

function tone(status: string) {
    return TONE[status] ?? { color: 'neutral' as const, dot: 'bg-dimmed' }
}

function when(run: Run): string {
    const at = run.completed_at ?? run.started_at
    return at ? relativeTime(new Date(at)) : ''
}
</script>

<template>
    <OverviewSection title="Just happened"
                     link-label="All executions"
                     link-to="/executions/runs">
        <div class="overflow-hidden rounded-lg border border-default divide-y divide-default">
            <NuxtLink v-for="run in runs"
                      :key="run.id"
                      :to="`/executions/runs/${run.id}`"
                      class="flex items-center gap-3 px-4 py-[11px] text-highlighted transition-colors hover:bg-muted">
                <span class="size-2 shrink-0 rounded-full"
                      :class="tone(run.status).dot" />
                <span class="min-w-0 flex-1 truncate font-mono text-[12.5px]">{{ run.component_name ?? run.component_key ?? 'Deleted target' }}</span>
                <span class="whitespace-nowrap text-xs text-dimmed">{{ run.partition_key ?? '' }}</span>
                <StatusPill :label="run.status"
                            :color="tone(run.status).color"
                            :dot="false"
                            class="w-16 justify-center capitalize" />
                <span class="w-14 whitespace-nowrap text-right text-xs text-dimmed">{{ when(run) }}</span>
            </NuxtLink>
            <div v-if="!runs.length"
                 class="px-4 py-5 text-sm text-muted">No runs yet.</div>
        </div>
    </OverviewSection>
</template>
```

- [ ] **Step 3: Components inventory**

```vue
<script setup lang="ts">
import type { KindInventory } from '~/types/overview'

const props = defineProps<{ rows: KindInventory[] }>()

const KIND_META: Record<string, { label: string, icon: string, to: string }> = {
    source: { label: 'Sources', icon: 'i-lucide-plug', to: kindPath('source') },
    asset: { label: 'Assets', icon: 'i-lucide-box', to: '/collection' },
    destination: { label: 'Destinations', icon: 'i-lucide-database', to: kindPath('destination') },
    connection: { label: 'Connections', icon: 'i-lucide-key-round', to: kindPath('connection') },
    job: { label: 'Jobs', icon: 'i-lucide-calendar-clock', to: kindPath('job') },
    hook: { label: 'Hooks', icon: 'i-carbon-lightning', to: kindPath('hook') },
}

const STATES = [
    { key: 'failing', label: 'failing', class: 'bg-error' },
    { key: 'attention', label: 'needs attention', class: 'bg-warning' },
    { key: 'healthy', label: 'healthy', class: 'bg-success' },
    { key: 'disabled', label: 'disabled', class: 'bg-accented' },
] as const

const table = computed(() => props.rows.map(row => ({
    ...row,
    meta: KIND_META[row.kind] ?? { label: kindLabel(row.kind), icon: 'i-lucide-box', to: kindPath(row.kind) },
    segments: STATES.filter(s => row[s.key]).map(s => ({
        ...s,
        pct: (100 * row[s.key]) / row.total,
        title: `${row[s.key]} ${s.label}`,
    })),
    issues: STATES.filter(s => s.key !== 'healthy' && row[s.key]).map(s => `${row[s.key]} ${s.label}`).join(' · ') || 'all healthy',
    issuesClass: row.failing ? 'text-error' : row.attention ? 'text-warning' : 'text-dimmed',
})))

const summary = computed(() => {
    const total = props.rows.reduce((n, r) => n + r.total, 0)
    const problems = props.rows.reduce((n, r) => n + r.failing + r.attention, 0)
    return `${total} in the collection · ${problems} need attention`
})
</script>

<template>
    <OverviewSection title="Components"
                     :meta="summary"
                     link-label="Open collection"
                     link-to="/collection">
        <div class="overflow-hidden rounded-lg border border-default">
            <div class="grid grid-cols-[minmax(130px,180px)_44px_minmax(0,1fr)_230px] items-center gap-x-5 border-b border-default bg-muted px-[18px] py-[9px] text-[11px] font-semibold uppercase tracking-[.06em] text-dimmed">
                <span>Kind</span><span class="text-right">Count</span><span>State</span><span class="text-right">Issues</span>
            </div>
            <NuxtLink v-for="row in table"
                      :key="row.kind"
                      :to="row.meta.to"
                      class="grid h-11 grid-cols-[minmax(130px,180px)_44px_minmax(0,1fr)_230px] items-center gap-x-5 border-b border-muted px-[18px] text-highlighted transition-colors hover:bg-muted">
                <span class="flex min-w-0 items-center gap-2.5 text-[13.5px] font-medium">
                    <UIcon :name="row.meta.icon"
                           class="size-4 shrink-0 text-dimmed" />{{ row.meta.label }}
                </span>
                <span class="text-right text-sm font-semibold tabular-nums">{{ row.total }}</span>
                <div class="flex h-[9px] gap-0.5 overflow-hidden rounded-full bg-accented">
                    <div v-for="segment in row.segments"
                         :key="segment.key"
                         :class="segment.class"
                         :style="{ width: `${segment.pct}%` }"
                         :title="segment.title" />
                </div>
                <span class="text-right text-xs leading-snug"
                      :class="row.issuesClass">{{ row.issues }}</span>
            </NuxtLink>
            <div class="flex items-center gap-4 bg-muted px-[18px] py-2.5 text-[11.5px] text-dimmed">
                <span v-for="state in STATES"
                      :key="state.key"
                      class="inline-flex items-center gap-1.5"><span class="size-[9px] rounded-sm"
                                                                     :class="state.class" />{{ state.label.replace(/^\w/, c => c.toUpperCase()) }}</span>
            </div>
        </div>
    </OverviewSection>
</template>
```

- [ ] **Step 4: Lint and typecheck**

Run: `pnpm run lint && pnpm exec nuxt typecheck`

---

### Task 12: Full check suite

- [ ] **Step 1: Python**

Run from the repo root: `uv run ruff check && uv run ty check && uv run pytest packages/interloper-db packages/interloper-api`
Expected: all green.

- [ ] **Step 2: Frontend**

Run from `packages/interloper-app/app/`: `pnpm run lint && pnpm exec nuxt typecheck`
Expected: clean.

---

### Task 13: Live verification on a seeded dev instance

Use the `verify` skill (`/verify`): it stands up a seeded instance on a non-3000 port and drives it headlessly.

- [ ] **Step 1: Start the instance**

`INTERLOPER_SERVER_PORT=3100 make dev-up` from the worktree root (needs `.env` copied from the main checkout; see the AGENTS.md "Local dev instance" section). A session from a `:3000` login is reused.

- [ ] **Step 2: Seed activity**

Through the UI or API: trigger the demo job a few times, let one fail (disable the demo source's asset `b` mid-run, or trigger a run on a partition the demo source rejects), create a backfill over 4 days. The exact recipe is in the `verify` skill.

- [ ] **Step 3: Check every section, light and dark**

On `http://localhost:3100/`:
- Health strip: four tiles with numbers that match `/executions/runs`, sparkline bars where runs completed, tiles link where the design says.
- Needs attention: error group row with "Open run", an overdue job after stamping `next_run_at` an hour back on the seeded job (SQL or the API), "All clear" when nothing is wrong.
- Timeline: hatched future third, "Now" pill, a dashed ghost at the seeded job's `next_run_at`, window tabs switch the span, `/timeline` unchanged.
- Coverage: cells coloured, click a day, the panel lists the job with Backfill (as editor) or Open run; the 3/6/12 tabs resize cells; the job filter narrows.
- Coming up / Just happened: times in the profile's display timezone.
- Components: six rows, bars sum to 100%, legend.
- Dark mode: no white boxes, no unreadable text.
- Sidebar: Overview is first and active only on `/`.

- [ ] **Step 4: Record**

Save screenshots (light and dark) in the scratchpad and summarise what matched the design and what did not, for the user's review. Do not commit.

---

## Self-review

- **Spec coverage.** Health strip (Task 4 + 7), Needs attention incl. Reconnect and All clear (Tasks 4, 8), Timeline with ghosts, hatching and Now marker (Task 9), Coverage calendar with day rules, granularity roll-up, job filter, windows, day detail with Backfill / Open run (Tasks 3, 5, 10), Coming up with partition ranges and Just happened (Tasks 4, 11), Components with state precedence (Tasks 4, 11), page + nav (Task 6), role gating (Tasks 8, 10), dark mode (every component uses semantic classes or `CHART_*` colours), realtime refresh (Task 6 store), tests in the mirrored locations (Tasks 1 to 5).
- **Placeholders.** The stub components in Task 6 step 5 are explicitly replaced in Tasks 7 to 11. No TBDs remain.
- **Type consistency.** `latest_by_target` (Task 1) is what Task 4 calls; `latest_by_component(org_id, event_types=...)` (Task 2) matches Task 4; `coverage_rows(org_id, since, until)` and `CoverageRow` (Task 3) match Task 5; `OverviewResponse` fields match `types/overview.ts`; `setFutureRatio` (Task 9 store) matches the section; `initialRange` (Task 10) matches the day detail; `relativeTime` and `OverviewSection` (Task 6) match Tasks 8 to 11.
