# Backfill Concurrency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A cron job's firing is a backfill gated by the job's own `concurrency`, through the same fan-out an API-created backfill uses.

**Architecture:** A backfill is gated at creation: the newest `concurrency` partitions start `queued`,
the rest `pending`, and `_advance_backfill` promotes on each completion. That split moves out of
`RunStore.create_backfill` into one session-level function, `create_backfill_runs`, which cron calls
too, stamping the job's `concurrency` on the row it still builds inline (atomic with the job's state
advance, no quota check on top-ups). `_advance_backfill` also learns that a `dispatched` run is in
flight. Nothing changes in the queue worker, the runner or the app.

**Tech Stack:** Python 3.10+, pydantic, SQLModel, SQLAlchemy, pytest, ruff, ty.

Spec: `docs/superpowers/specs/2026-09-29-backfill-concurrency-design.md`.

## Global Constraints

- Line length 120, ruff-formatted. Type-checked with `ty`.
- Google-style docstrings on every module, class, function and method, with every applicable section.
- Tests mirror the package layout one to one; new tests go into the existing module test files.
- No attribute-level comments on fields; the why lives in one docstring or spec.
- No migration: `Backfill.concurrency` exists and job config is JSON.
- Run checks from the repo root: `uv run ruff check`, `uv run ty check`, `uv run pytest`.
  Prefix `UV_FROZEN=1` to keep `uv.lock` untouched.
- **Do not commit without Guillaume asking.** Each commit step records the intended message only.
- Do not use "—" in any text.

---

### Task 1: `CronJob.concurrency`

**Files:**
- Modify: `packages/interloper-core/src/interloper/job/cron.py:41-52` (after the `offset` field)
- Modify: `docs/guide/jobs.md:48-54` (the `CronJob` field table)
- Test: `packages/interloper-core/tests/job/test_cron.py`
- Test: `packages/interloper-core/tests/job/test_base.py:72`

**Interfaces:**
- Consumes: nothing.
- Produces: `CronJob.concurrency: int` (default `1`, `ge=1`), serialised into a job's config JSON
  as `"concurrency"`. Task 4 reads it with `config.get("concurrency", 1)`.

- [ ] **Step 1: Write the failing tests**

Append to `packages/interloper-core/tests/job/test_cron.py`:

```python
class TestConcurrency:
    def test_defaults_to_one(self) -> None:
        assert CronJob(cron="0 6 * * *").concurrency == 1

    def test_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="greater than or equal to 1"):
            CronJob(cron="0 6 * * *", concurrency=0)

    def test_sits_in_the_operation_section(self) -> None:
        prop = CronJob.config_schema()["properties"]["concurrency"]
        assert prop["x-section"] == "Operation"
        assert prop["title"] == "Concurrency"
```

In `packages/interloper-core/tests/job/test_base.py`, change the assertion at line 72 to:

```python
        assert list(properties) == [
            "cron",
            "timezone",
            "lookback",
            "offset",
            "concurrency",
            "tags",
            "enabled",
            "retry",
        ]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/job -q`
Expected: `TestConcurrency` fails with `ValidationError` / `KeyError: 'concurrency'`, and the
`test_base.py` order assertion fails.

- [ ] **Step 3: Declare the field**

In `packages/interloper-core/src/interloper/job/cron.py`, after the `offset` field:

```python
    concurrency: int = Field(
        default=1,
        ge=1,
        title="Concurrency",
        description="How many partitions of one firing run at once",
        json_schema_extra={"x-section": "Operation"},
    )
```

Extend the class docstring's window paragraph (the one ending with the reference to
`TimePartitionWindow.lookback`) with one sentence:

```
    Each firing is a backfill over that window, and ``concurrency`` is how many
    of its partitions are in flight at once, newest first.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/job -q`
Expected: PASS.

- [ ] **Step 5: Document the field**

In `docs/guide/jobs.md`, add a row to the `CronJob` field table after `offset`:

```markdown
| `concurrency` | `1` | How many partitions of one firing run at once. |
```

and append to the paragraph that follows the table (after "never stored."):

```markdown
Each firing is a backfill over the window, dispatched newest partition first and gated by
`concurrency`: at `1` the partitions run one at a time.
```

- [ ] **Step 6: Record the commit**

Do not run this; it records the intended message for when Guillaume asks:

```bash
git add packages/interloper-core/src/interloper/job/cron.py packages/interloper-core/tests/job docs/guide/jobs.md
git commit -m "feat(core): declare a cron job's backfill concurrency"
```

---

### Task 2: `create_backfill_runs`

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py` (`create_backfill` at :422-508, new function before `cancel_backfill_runs` at :785)
- Test: `packages/interloper-db/tests/store/test_runs.py` (a new class after `TestCreateBackfill`, which ends at :492)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `create_backfill_runs(session: Session, db_backfill: Backfill, window: TimePartitionWindow) -> None`,
  a module-level function in `interloper_db.store.runs`. It requires the backfill row to be added
  and flushed (so `db_backfill.id` is set) and reads `org_id`, `component_id` and `concurrency` from
  it. Task 4 imports it.

- [ ] **Step 1: Write the failing test**

Add to `packages/interloper-db/tests/store/test_runs.py`, after `TestCreateBackfill`:

```python
class TestCreateBackfillRuns:
    """The fan-out every backfill shares: newest `concurrency` queued, the rest pending."""

    def test_queues_the_newest_partitions_and_leaves_the_rest_pending(self, store: Store):
        window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 4))
        with Session(store.engine) as session:
            db_backfill = Backfill(
                org_id=_ORG_ID, start_key="2026-01-01", end_key="2026-01-04", concurrency=2, status="running"
            )
            session.add(db_backfill)
            session.flush()

            create_backfill_runs(session, db_backfill, window)
            session.commit()
            backfill_id = db_backfill.id

        assert store.runs.get_backfill(backfill_id).partitions == 4
        assert _partition_statuses(store, backfill_id) == {
            "2026-01-01": "pending",
            "2026-01-02": "pending",
            "2026-01-03": "queued",
            "2026-01-04": "queued",
        }

    def test_creates_rows_oldest_first(self, store: Store):
        window = il.TimePartitionWindow(dt.date(2026, 1, 1), dt.date(2026, 1, 3))
        with Session(store.engine) as session:
            db_backfill = Backfill(
                org_id=_ORG_ID, start_key="2026-01-01", end_key="2026-01-03", concurrency=1, status="running"
            )
            session.add(db_backfill)
            session.flush()
            create_backfill_runs(session, db_backfill, window)
            session.commit()
            runs = session.exec(
                select(Run).where(Run.backfill_id == db_backfill.id).order_by(col(Run.created_at))
            ).all()
        assert [run.partition_key for run in runs] == ["2026-01-01", "2026-01-02", "2026-01-03"]
```

Add the import at the top of the test module, with the other `interloper_db` imports:

```python
from interloper_db.store.runs import create_backfill_runs
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/store/test_runs.py -q`
Expected: collection fails with `ImportError: cannot import name 'create_backfill_runs'`.

- [ ] **Step 3: Write the function and call it from `create_backfill`**

In `packages/interloper-db/src/interloper_db/store/runs.py`, add before `cancel_backfill_runs`:

```python
def create_backfill_runs(session: Session, db_backfill: Backfill, window: TimePartitionWindow) -> None:
    """Create a backfill's runs: one per partition, the newest ``concurrency`` of them queued.

    Part of the caller's transaction (the caller commits), on a backfill row
    already flushed so the runs can reference it. Rows are created oldest
    first, so a runs list ordered by ``created_at`` desc keeps the newest
    partition on top, while the *newest* ``concurrency`` of them are ``queued``
    and the rest wait ``pending``; ``_advance_backfill`` promotes in the same
    newest-first order, so the freshest data lands first and an interrupted
    backfill keeps the recent window rather than the ancient tail.

    Args:
        session: Active database session (the caller commits).
        db_backfill: The flushed backfill row the runs belong to; its
            ``partitions`` count is stamped here.
        window: The partitions the backfill covers.
    """
    span = window.partition_count()
    first_queued = max(0, span - db_backfill.concurrency)
    for index, value in enumerate(window.granularity.period_range(window.start, window.end)):
        session.add(
            Run(
                org_id=db_backfill.org_id,
                component_id=db_backfill.component_id,
                backfill_id=db_backfill.id,
                partition_key=window.granularity.format(value),
                status="queued" if index >= first_queued else "pending",
            )
        )
    db_backfill.partitions = span
    session.add(db_backfill)
```

In `create_backfill`, replace everything from the comment that begins `# Rows are created
oldest-first` down to and including `session.add(db_backfill)` (the block before `commit(session)`)
with:

```python
            create_backfill_runs(session, db_backfill, window)
```

Trim the method docstring's second paragraph to point at the function rather than repeat it:

```
        The bounds are partition keys whose shape carries the granularity
        (``2026-08-21``, ``2026-08``, ``2026``, ``2026-08-21T13``), so a
        monthly backfill is just two month keys. The runs are fanned out by
        :func:`create_backfill_runs`: newest partition first, ``concurrency``
        of them queued at once.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/store/test_runs.py -q`
Expected: PASS, including the untouched `TestCreateBackfill` cases.

- [ ] **Step 5: Record the commit**

```bash
git add packages/interloper-db/src/interloper_db/store/runs.py packages/interloper-db/tests/store/test_runs.py
git commit -m "refactor(db): fan a backfill's runs out through one function"
```

---

### Task 3: A dispatched run is in flight

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py:709-750` (`_advance_backfill`)
- Test: `packages/interloper-db/tests/store/test_runs.py` (`TestBackfillProgression`, at :741)

**Interfaces:**
- Consumes: nothing.
- Produces: `_advance_backfill` counts `queued`, `dispatched` and `running` runs as in flight.

- [ ] **Step 1: Write the failing tests**

Add to `TestBackfillProgression` in `packages/interloper-db/tests/store/test_runs.py`:

```python
    def test_a_dispatched_run_holds_its_slot(self, store: Store):
        # Between the queue's claim and the pod's first write a run is
        # `dispatched`: still occupying its slot, not yet `running`.
        backfill = store.runs.create_backfill(
            _ORG_ID, start_key="2026-01-01", end_key="2026-01-04", concurrency=2
        )
        _mark_dispatched(store, backfill.id)
        second = _mark_dispatched(store, backfill.id)

        store.runs.complete(second, success=True)

        statuses = _partition_statuses(store, backfill.id)
        assert list(statuses.values()).count("queued") == 1
        assert statuses["2026-01-01"] == "pending"

    def test_a_dispatched_run_keeps_the_backfill_open(self, store: Store):
        backfill = store.runs.create_backfill(
            _ORG_ID, start_key="2026-01-01", end_key="2026-01-02", concurrency=2
        )
        _mark_dispatched(store, backfill.id)
        second = _mark_dispatched(store, backfill.id)

        store.runs.complete(second, success=True)

        assert store.runs.get_backfill(backfill.id).status == "running"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/store/test_runs.py -q -k dispatched`
Expected: both FAIL (two partitions promoted; backfill finalised as `success`).

- [ ] **Step 3: Count dispatched runs**

In `_advance_backfill`, change the in-flight query's status list:

```python
                    col(Run.status).in_(["queued", "dispatched", "running"]),
```

and the docstring's step 2 to read:

```
        2. **Finalize**: if nothing in-flight or pending, mark complete. In
           flight is queued, dispatched or running: a claimed run occupies its
           slot before its pod first writes. The verdict reads each stack's
           latest attempt, so an attempt a later one healed no longer condemns
           the batch. A queued successor still counts as in flight, which is
           what keeps the batch open while a retry waits out its backoff.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/store/test_runs.py -q`
Expected: PASS.

- [ ] **Step 5: Record the commit**

```bash
git add packages/interloper-db/src/interloper_db/store/runs.py packages/interloper-db/tests/store/test_runs.py
git commit -m "fix(db): count a dispatched run as in flight when advancing a backfill"
```

---

### Task 4: Cron fans out through the same function

**Files:**
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/cron.py:12-32` (imports) and `:140-177` (the inline backfill)
- Modify: `docs/guide/backfilling.md` (new section after "Trailing windows")
- Modify: `docs/ui/index.md:108`
- Test: `packages/interloper-scheduler/tests/test_cron.py:118-136`

**Interfaces:**
- Consumes: `create_backfill_runs(session, db_backfill, window)` from Task 2;
  `"concurrency"` in the job's config from Task 1.
- Produces: cron backfills whose row carries the job's `concurrency` and whose runs are gated by it.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-scheduler/tests/test_cron.py`, replace the last three lines of
`test_partitioned_job_creates_a_backfill_window` (from `runs = _runs(store)`) with:

```python
        runs = _runs(store)
        assert len(runs) == 3
        assert all(run.backfill_id == backfill.id for run in runs)
```

and add these tests right after it:

```python
    def test_a_firing_is_gated_by_the_jobs_concurrency(self, store: Store) -> None:
        store = _catalog_store()
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 3},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.concurrency == 1
        statuses = {run.partition_key: run.status for run in _runs(store)}
        assert statuses[backfill.end_key] == "queued"
        assert list(statuses.values()).count("pending") == 2

    def test_a_concurrency_covering_the_window_queues_every_partition(self, store: Store) -> None:
        store = _catalog_store()
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 3, "concurrency": 3},
        )
        CronController(store=store)._tick()

        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        assert backfill.concurrency == 3
        assert all(run.status == "queued" for run in _runs(store))

    def test_completing_a_gated_run_promotes_the_next_newest(self, store: Store) -> None:
        store = _catalog_store()
        _job_targeting(
            store,
            "daily_source",
            config={"cron": "0 * * * *", "enabled": True, "lookback": 3},
        )
        CronController(store=store)._tick()
        queued = next(run for run in _runs(store) if run.status == "queued")
        assert queued.id is not None

        store.runs.complete(queued.id, success=True)

        statuses = {run.partition_key: run.status for run in _runs(store)}
        with Session(store.engine) as session:
            backfill = session.exec(select(Backfill)).one()
        next_newest = (dt.date.fromisoformat(backfill.end_key) - dt.timedelta(days=1)).isoformat()
        assert statuses[next_newest] == "queued"
        assert statuses[backfill.start_key] == "pending"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-scheduler/tests/test_cron.py -q`
Expected: the gating and promotion tests FAIL (every run `queued`, row `concurrency` untouched by
the job); the covering-window test passes by accident and stays as the explicit case.

- [ ] **Step 3: Call the function from cron**

In `packages/interloper-scheduler/src/interloper_scheduler/cron.py`, add the import after the
`interloper_db.models` line:

```python
from interloper_db.store.runs import create_backfill_runs
```

Replace the block from the comment `# Create runs. The backfill is built inline rather than via`
through `session.add(backfill)` (the line before `else:`) with:

```python
                # The backfill row is built inline rather than through
                # Store.runs.create_backfill: it must commit atomically with
                # the job's state advance (a crash between the two would
                # re-create it next tick), and a top-up skips the quota
                # checks a user-created backfill pays. The fan-out itself is
                # the shared one, gated by the job's concurrency.
                try:
                    window = self._backfill_window(session, job, config, now.astimezone(zone))
                except ValueError as exc:
                    # Targets disagree on granularity: skip rather than
                    # backfill a window that is wrong for some of them.
                    logger.error("Skipping job '%s': %s", job.name, exc)
                    continue
                if window is not None:
                    backfill = Backfill(
                        org_id=job.org_id,
                        component_id=job.id,
                        start_key=window.granularity.format(window.start),
                        end_key=window.granularity.format(window.end),
                        concurrency=config.get("concurrency", 1),
                        status="running",
                        started_at=now,
                    )
                    session.add(backfill)
                    session.flush()
                    create_backfill_runs(session, backfill, window)
```

`Run` stays imported: the unpartitioned branch still builds one.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-scheduler -q`
Expected: PASS.

- [ ] **Step 5: Document scheduled backfills**

In `docs/guide/backfilling.md`, add after the "Trailing windows" section (before "Bounded history"):

```markdown
## Scheduled backfills

A cron job's firing is a backfill over its trailing window, the same as one queued by hand: one
run per partition, dispatched newest first, with at most the job's `concurrency` of them in flight
at once. A retry waiting out its delay keeps its partition's slot.
```

In `docs/ui/index.md`, change line 108 to:

```markdown
the partition-window fields (`lookback`, `offset`); its Operation section carries `concurrency`,
how many of a firing's partitions run at once.
```

- [ ] **Step 6: Record the commit**

```bash
git add packages/interloper-scheduler/src/interloper_scheduler/cron.py packages/interloper-scheduler/tests/test_cron.py docs/guide/backfilling.md docs/ui/index.md
git commit -m "feat(scheduler): gate a cron job's firing by its concurrency"
```

---

### Task 5: Full checks

**Files:** none new.

- [ ] **Step 1: Run the Python checks**

Run from the repo root:

```bash
UV_FROZEN=1 uv run ruff check packages
UV_FROZEN=1 uv run ruff format --check packages/interloper-core/src/interloper/job/cron.py packages/interloper-db/src/interloper_db/store/runs.py packages/interloper-scheduler/src/interloper_scheduler/cron.py
UV_FROZEN=1 uv run ty check
UV_FROZEN=1 uv run pytest
```

Expected: all pass. The app needs no change; skip its checks.

- [ ] **Step 2: Report**

Summarise to Guillaume: the four commits above (messages recorded, not run), the behaviour change
(cron top-ups gated at 1 unless the job says otherwise), and that the Swarovski jobs need
`concurrency` set after the deploy.
