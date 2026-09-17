# Retry Phase 2: Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A failed run queues its own next attempt, backfills and hooks report the stack's verdict rather than any one attempt's.

**Architecture:** There is no instance-wide default in this phase, so a run retries only when its
target job declares a policy; phases 1 and 2 therefore change no behaviour until something declares
one. A run attempt is a `Run` row; a stack is the attempts of one unit of work, keyed by
a new `root_run_id`. `RunStore.complete()` is the single terminal path every failure takes, so it is
where the successor is created, in the same transaction that marks the run failed. That makes "has a
successor" true the instant the run fails, which is what lets the hook evaluator gate on it without
knowing anything about budgets. A `scheduled_for` column holds the backoff, honoured by the queue's
claim. Backfill accounting and the `executions` view both move from counting attempts to reading the
latest one.

**Tech Stack:** Python 3.10+, SQLModel, SQLAlchemy, Alembic, PostgreSQL, pytest, ruff, ty.

Spec: `docs/superpowers/specs/2026-09-17-retry-design.md`, sections 5 to 8 and 10.
Prerequisite: `docs/superpowers/plans/2026-09-17-retry-phase-1-core.md` must be complete.
`RetryPolicy` and `Job.retry` exist.

## Global Constraints

- Line length 120, ruff-formatted. Type-checked with `ty`.
- Google-style docstrings on every module, class, function and method, with every applicable section.
  An Alembic revision's `upgrade`/`downgrade` pair is exempt; the module docstring carries the intent.
- Tests mirror the package layout one to one.
- The migration chain was folded to `001`-`003`, so this is revision `004` with `down_revision = "003"`.
- `env.py` uses `transaction_per_migration` and commits after `SET lock_timeout`; do not add a
  `CREATE INDEX CONCURRENTLY` without checking that file first.
- Run checks from the repo root: `uv run ruff check`, `uv run ty check`, `uv run pytest`.
- **Do not commit without Guillaume asking.** The commit step records the intended message.

---

### Task 1: Stack and schedule columns

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/models/runs.py:52-96`
- Create: `packages/interloper-db/src/interloper_db/migrations/versions/004_run_attempts.py`
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py` (`create` at :51, `create_backfill` at :335, `retry` at :286)
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/cron.py:155-175`
- Test: `packages/interloper-db/tests/store/test_runs.py`

**Interfaces:**
- Consumes: nothing from phase 1.
- Produces: `Run.root_run_id: UUID` (not null, its own id for a first attempt) and
  `Run.scheduled_for: datetime | None`. Every run-creating call site assigns `id=uuid4()` explicitly
  so the root can be set in the same statement. Tasks 2 to 5 read both columns.

- [ ] **Step 1: Write the failing test**

Add to `packages/interloper-db/tests/store/test_runs.py`:

```python
def test_a_new_run_is_its_own_stack_root(store, org):
    run = store.runs.create(org.id, component_id=None)

    assert run.root_run_id == run.id
    assert run.scheduled_for is None


def test_backfill_runs_are_each_their_own_root(store, org):
    backfill = store.runs.create_backfill(org.id, start_key="2026-01-01", end_key="2026-01-03")

    runs = store.runs.list_all(org.id, backfill_id=backfill.id)
    assert {run.root_run_id for run in runs} == {run.id for run in runs}
```

Use the fixtures the surrounding tests in that module already use for `store` and `org`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v -k "stack_root or own_root"`
Expected: FAIL with `AttributeError: 'Run' object has no attribute 'root_run_id'`.

- [ ] **Step 3: Add the columns to the model**

In `packages/interloper-db/src/interloper_db/models/runs.py`, add to `Run`, after `retry_scope`:

```python
    root_run_id: UUID = SQLField(
        sa_column=Column(ForeignKey("runs.id", ondelete="SET NULL"), index=True, nullable=False),
    )
    scheduled_for: datetime | None = SQLField(default=None, sa_column=Column(TZDateTime))
```

Extend the class docstring with one paragraph: a run is one attempt, `root_run_id` groups the
attempts of one unit of work into a stack and is the run's own id for a first attempt, and
`scheduled_for` is the earliest instant the queue may claim it.

- [ ] **Step 4: Assign ids and roots at every creation site**

`root_run_id` cannot default to the row's own server-generated id, so the id is assigned in Python.
In `packages/interloper-db/src/interloper_db/store/runs.py`, in `create`:

```python
            run_id = uuid4()
            db_run = Run(
                id=run_id,
                root_run_id=run_id,
                org_id=org_id,
                component_id=component_id,
                partition_key=partition_key,
                status="queued",
                billable=billable,
            )
```

In `create_backfill`, inside the per-partition loop:

```python
                run_id = uuid4()
                db_run = Run(
                    id=run_id,
                    root_run_id=run_id,
                    org_id=org_id,
                    component_id=component_id,
                    backfill_id=db_backfill.id,
                    partition_key=window.granularity.format(value),
                    status="queued" if index >= first_queued else "pending",
                )
```

In `retry`, the successor joins its predecessor's stack:

```python
            db_run = Run(
                id=uuid4(),
                root_run_id=src.root_run_id,
                org_id=src.org_id,
                ...
            )
```

In `packages/interloper-scheduler/src/interloper_scheduler/cron.py`, both `Run(...)` constructions
(the per-partition one and the unpartitioned one) get the same `run_id = uuid4()` treatment.

Add `from uuid import uuid4` where it is missing.

- [ ] **Step 5: Write the migration**

Create `packages/interloper-db/src/interloper_db/migrations/versions/004_run_attempts.py`:

```python
"""Add the run stack and its schedule.

A run is one attempt. ``root_run_id`` groups the attempts of one unit of work
so a stack is a single indexed predicate rather than a recursive walk, and is
the run's own id for a first attempt. ``scheduled_for`` holds a retry's
backoff: the queue claims a run only once it has passed.

Existing rows are folded into stacks by walking the ``retry_of`` chains that
manual retries already created. A run whose predecessor was deleted becomes
its own root, which is correct: its lineage is gone.
"""

from alembic import op

revision: str = "004"
down_revision: str | None = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE runs ADD COLUMN root_run_id uuid REFERENCES runs(id) ON DELETE SET NULL")
    op.execute("ALTER TABLE runs ADD COLUMN scheduled_for timestamptz")
    op.execute(
        """
        WITH RECURSIVE chain AS (
            SELECT id, id AS root FROM runs WHERE retry_of IS NULL
            UNION ALL
            SELECT r.id, c.root FROM runs r JOIN chain c ON r.retry_of = c.id
        )
        UPDATE runs SET root_run_id = chain.root FROM chain WHERE runs.id = chain.id
        """
    )
    op.execute("UPDATE runs SET root_run_id = id WHERE root_run_id IS NULL")
    op.execute("ALTER TABLE runs ALTER COLUMN root_run_id SET NOT NULL")
    op.execute("CREATE INDEX ix_runs_root_run_id ON runs (root_run_id)")
    op.execute("CREATE INDEX ix_runs_claim ON runs (status, scheduled_for, created_at)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_runs_claim")
    op.execute("DROP INDEX IF EXISTS ix_runs_root_run_id")
    op.execute("ALTER TABLE runs DROP COLUMN scheduled_for")
    op.execute("ALTER TABLE runs DROP COLUMN root_run_id")
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v`
Expected: PASS.

- [ ] **Step 7: Verify the migration applies to an empty and a populated database**

Run: `make dev-reset`
Expected: the chain migrates to `004` and the seed succeeds.

Then apply it to a database that already holds runs, which is what exercises the recursive backfill.
Do not run this against the shared dev database if another session may be using it; use a throwaway.

- [ ] **Step 8: Commit**

```bash
git add packages/interloper-db packages/interloper-scheduler/src/interloper_scheduler/cron.py
git commit -m "feat(db): give every run a stack root and a schedule

By Digitl"
```

---

### Task 2: A failed run queues its next attempt

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py:245-284` (`complete`) and a new `_plan_retry`
- Test: `packages/interloper-db/tests/store/test_runs.py`

**Interfaces:**
- Consumes: `Run.root_run_id`/`Run.scheduled_for` (Task 1) and `Job.retry` (phase 1).
- Produces: `RunStore._plan_retry(session, db_run) -> Run | None`, called from `complete` on failure
  before `_advance_backfill`. Tasks 4 and 6 rely on the successor existing by the time the
  transaction commits.

- [ ] **Step 1: Write the failing tests**

Add to `packages/interloper-db/tests/store/test_runs.py`:

```python
def test_a_failed_run_queues_its_next_attempt(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 2, "delay": 60}})
    run = store.runs.create(org.id, component_id=job_component.id)

    store.runs.complete(run.id, success=False)

    successor = store.runs.list_all(org.id, component_id=job_component.id)[0]
    assert successor.id != run.id
    assert successor.retry_of == run.id
    assert successor.root_run_id == run.root_run_id
    assert successor.attempt == 2
    assert successor.retry_scope == "failed"
    assert successor.status == "queued"
    assert successor.scheduled_for is not None


def test_an_exhausted_budget_queues_nothing(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 1}})
    run = store.runs.create(org.id, component_id=job_component.id)

    store.runs.complete(run.id, success=False)

    assert len(store.runs.list_all(org.id, component_id=job_component.id)) == 1


def test_a_successful_run_queues_nothing(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 3}})
    run = store.runs.create(org.id, component_id=job_component.id)

    store.runs.complete(run.id, success=True)

    assert len(store.runs.list_all(org.id, component_id=job_component.id)) == 1


def test_a_run_whose_target_is_gone_queues_nothing(store, org):
    run = store.runs.create(org.id, component_id=None)

    store.runs.complete(run.id, success=False)

    assert len(store.runs.list_all(org.id)) == 1


def test_a_backfill_run_keeps_its_backfill_when_it_retries(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 2}})
    backfill = store.runs.create_backfill(
        org.id, component_id=job_component.id, start_key="2026-01-01", end_key="2026-01-01"
    )
    run = store.runs.list_all(org.id, backfill_id=backfill.id)[0]

    store.runs.complete(run.id, success=False)

    successor = [r for r in store.runs.list_all(org.id, backfill_id=backfill.id) if r.id != run.id][0]
    assert successor.backfill_id == backfill.id
```

Add a `job_component` fixture to that module if one does not exist, creating a `job`-kind component
row for the org through the same path the surrounding tests use.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v -k "next_attempt or exhausted or queues_nothing or keeps_its_backfill"`
Expected: FAIL. Nothing creates a successor.

- [ ] **Step 3: Write the retry planner**

In `packages/interloper-db/src/interloper_db/store/runs.py`, add to `RunStore`, in the internals
section:

```python
    @staticmethod
    def _retry_policy(session: Session, db_run: Run) -> il.RetryPolicy | None:
        """The run-level policy in force for a run.

        A job's declared policy governs its runs, and nothing else does: a
        source's or an asset's own ``retry`` is an operation policy, and
        reading it here would apply an operation's budget to whole runs. There
        is no instance-wide fallback, so a run whose target declares nothing is
        attempted once. The config column is plain JSON, so this is one row
        read and no hydration.

        Args:
            session: Open session the target row is read through.
            db_run: The run whose policy is resolved.

        Returns:
            The policy, or ``None`` when the target declares none.
        """
        if db_run.component_id is None:
            return None
        db_component = session.get(Component, db_run.component_id)
        if db_component is None or db_component.kind != "job":
            return None
        declared = (db_component.config or {}).get("retry")
        return il.RetryPolicy.model_validate(declared) if declared else None

    def _plan_retry(self, session: Session, db_run: Run) -> Run | None:
        """Queue the next attempt of a failed run, when its budget allows one.

        Called from the single terminal path, in the transaction that marks the
        run failed, so that a doomed attempt never looks final to anything
        reading the table. The successor stays in its predecessor's backfill and
        stack, and re-runs only what failed. The quota is deliberately not
        checked here: dispatch is the authoritative gate and cancels an
        over-quota run at claim time, like any other run.

        Args:
            session: Open session the successor is written through.
            db_run: The run that just failed.

        Returns:
            The queued successor, or ``None`` when nothing is retried.
        """
        if db_run.component_id is None:
            return None
        policy = self._retry_policy(session, db_run)
        if policy is None or not policy.allows(db_run.attempt + 1):
            return None

        successor = Run(
            id=uuid4(),
            org_id=db_run.org_id,
            component_id=db_run.component_id,
            backfill_id=db_run.backfill_id,
            partition_key=db_run.partition_key,
            status="queued",
            scheduled_for=datetime.now(timezone.utc)
            + timedelta(seconds=policy.delay_before(db_run.attempt + 1)),
            retry_of=db_run.id,
            root_run_id=db_run.root_run_id,
            attempt=db_run.attempt + 1,
            retry_scope="failed",
            billable=db_run.billable,
        )
        session.add(successor)
        session.flush()
        logger.info("Queued attempt %d of run stack %s", successor.attempt, successor.root_run_id)
        return successor
```

Add the imports it needs: `timedelta` and `uuid4`. `interloper as il` is already imported, which is
what `il.RetryPolicy` resolves through.

- [ ] **Step 4: Call it from the terminal path**

In `complete`, between the component stamp and the backfill advance:

```python
            if not success:
                self._plan_retry(session, db_run)

            if db_run.backfill_id:
                self._advance_backfill(session, db_run.backfill_id, failed=not success)
```

Extend `complete`'s docstring: on failure it also queues the next attempt when the target's policy
allows one, and it does so before advancing the backfill so that the batch's in-flight count sees the
successor and does not finalize early.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/interloper-db
git commit -m "feat(db): queue the next attempt when a run fails

By Digitl"
```

---

### Task 3: The queue honours the schedule

**Files:**
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/queue.py:79-116`
- Test: `packages/interloper-scheduler/tests/test_queue.py`

**Interfaces:**
- Consumes: `Run.scheduled_for` (Task 1).
- Produces: a claim that skips a run whose `scheduled_for` is still in the future. No new API.

- [ ] **Step 1: Write the failing test**

Add to `packages/interloper-scheduler/tests/test_queue.py`, following the module's existing way of
building a controller with a fake launcher:

```python
def test_a_scheduled_run_is_not_claimed_before_its_time(controller, store, org):
    run = store.runs.create(org.id, component_id=None)
    with Session(store.engine) as session:
        db_run = session.get(Run, run.id)
        db_run.scheduled_for = datetime.now(timezone.utc) + timedelta(hours=1)
        session.add(db_run)
        session.commit()

    assert controller._claim_next() is None


def test_a_run_whose_schedule_has_passed_is_claimed(controller, store, org):
    run = store.runs.create(org.id, component_id=None)
    with Session(store.engine) as session:
        db_run = session.get(Run, run.id)
        db_run.scheduled_for = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.add(db_run)
        session.commit()

    assert controller._claim_next() == run.id
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-scheduler/tests/test_queue.py -v -k scheduled`
Expected: FAIL. The first test claims the run anyway.

- [ ] **Step 3: Write the implementation**

In `packages/interloper-scheduler/src/interloper_scheduler/queue.py`, add the predicate to the claim
statement in `_claim_next`:

```python
                statement = (
                    select(Run)
                    .where(Run.status == "queued")
                    .where(col(Run.scheduled_for).is_(None) | (col(Run.scheduled_for) <= func.now()))
                    .order_by(col(Run.created_at).asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
```

Import `func` from sqlalchemy. Extend the method's docstring: a run carrying a schedule is not
claimable until it has passed, which is how a retry's backoff is served without a second status.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-scheduler/tests/test_queue.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-scheduler
git commit -m "feat(scheduler): claim a run only once its schedule has passed

By Digitl"
```

---

### Task 4: Backfills finalize on stacks

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/store/runs.py:584-643` (`_advance_backfill`)
- Test: `packages/interloper-db/tests/store/test_runs.py`

**Interfaces:**
- Consumes: `Run.root_run_id` (Task 1), `_plan_retry` (Task 2).
- Produces: a backfill whose terminal status reads the latest attempt of each stack. No new API.

- [ ] **Step 1: Write the failing test**

Add to `packages/interloper-db/tests/store/test_runs.py`:

```python
def test_a_backfill_healed_by_a_retry_succeeds(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 2, "delay": 0}})
    backfill = store.runs.create_backfill(
        org.id, component_id=job_component.id, start_key="2026-01-01", end_key="2026-01-01"
    )
    first = store.runs.list_all(org.id, backfill_id=backfill.id)[0]

    store.runs.complete(first.id, success=False)
    successor = [r for r in store.runs.list_all(org.id, backfill_id=backfill.id) if r.id != first.id][0]
    store.runs.complete(successor.id, success=True)

    assert store.runs.get_backfill(backfill.id).status == "success"


def test_a_backfill_whose_stack_exhausts_its_budget_fails(store, org, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 2, "delay": 0}})
    backfill = store.runs.create_backfill(
        org.id, component_id=job_component.id, start_key="2026-01-01", end_key="2026-01-01"
    )
    first = store.runs.list_all(org.id, backfill_id=backfill.id)[0]

    store.runs.complete(first.id, success=False)
    successor = [r for r in store.runs.list_all(org.id, backfill_id=backfill.id) if r.id != first.id][0]
    store.runs.complete(successor.id, success=False)

    assert store.runs.get_backfill(backfill.id).status == "failed"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v -k backfill_healed`
Expected: FAIL. The healed backfill is marked `failed`, because the first attempt's row still counts.

- [ ] **Step 3: Write the implementation**

In `_advance_backfill`, replace the `any_failed` lookup in the finalize branch:

```python
        if in_flight_count == 0 and len(pending_runs) == 0:
            latest = (
                select(Run.root_run_id, func.max(col(Run.attempt)).label("attempt"))
                .where(Run.backfill_id == backfill_id)
                .group_by(col(Run.root_run_id))
                .subquery()
            )
            any_failed = session.exec(
                select(Run)
                .join(
                    latest,
                    onclause=(col(Run.root_run_id) == latest.c.root_run_id)
                    & (col(Run.attempt) == latest.c.attempt),
                )
                .where(Run.backfill_id == backfill_id, Run.status == "failed")
            ).first()
            db_backfill.status = "failed" if any_failed else "success"
            db_backfill.completed_at = datetime.now(timezone.utc)
            session.add(db_backfill)
            return
```

Extend the method's docstring to say the verdict reads each stack's latest attempt, so an attempt
that a later one healed no longer condemns the batch.

Leave the in-flight count and the fail-fast branch alone: a queued successor already counts as
in-flight, which is exactly what keeps the batch open while a retry is pending.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-db/tests/store/test_runs.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/interloper-db
git commit -m "fix(db): finalize a backfill on its stacks, not its attempts

By Digitl"
```

---

### Task 5: The executions view reads the latest attempt

**Files:**
- Modify: `packages/interloper-db/src/interloper_db/migrations/versions/004_run_attempts.py`
- Modify: `packages/interloper-db/src/interloper_db/models/runs.py:166-186` (`Execution`)
- Test: `packages/interloper-db/tests/store/test_events.py`

**Interfaces:**
- Consumes: `EventType.OPERATION_RETRIED` and the `attempt` key in operation event data (phase 1).
- Produces: an `executions` view still keyed by `(run_id, component_id)`, whose `status` is the
  latest attempt's and which gains `attempts: int`. `Execution` gains the matching field. Phase 3
  renders it.

- [ ] **Step 1: Write the failing test**

Add to `packages/interloper-db/tests/store/test_events.py`, following the module's existing way of
saving events and reading executions back:

```python
def test_an_operation_that_healed_reads_as_a_success(store, org, run, component):
    store.events.save(_operation_event(EventType.OPERATION_FAILED, component, attempt=1), org_id=org.id, run_id=run.id)
    store.events.save(_operation_event(EventType.OPERATION_RETRIED, component, attempt=1), org_id=org.id, run_id=run.id)
    store.events.save(
        _operation_event(EventType.OPERATION_COMPLETED, component, attempt=2), org_id=org.id, run_id=run.id
    )

    executions = store.events.list_executions(run.id)
    assert len(executions) == 1
    assert executions[0].status == "success"
    assert executions[0].attempts == 2


def test_an_operation_that_exhausted_its_budget_reads_as_a_failure(store, org, run, component):
    store.events.save(_operation_event(EventType.OPERATION_RETRIED, component, attempt=1), org_id=org.id, run_id=run.id)
    store.events.save(_operation_event(EventType.OPERATION_FAILED, component, attempt=2), org_id=org.id, run_id=run.id)

    executions = store.events.list_executions(run.id)
    assert executions[0].status == "failed"
    assert executions[0].attempts == 2
```

Write the `_operation_event` helper in the test module: it builds an `il.Event` of the given type
whose metadata carries `component_id`, `component_kind`, `component_key` and `attempt`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-db/tests/store/test_events.py -v -k "healed or exhausted"`
Expected: FAIL. The healed operation reads `failed`, because the view ranks `operation_failed` above
`operation_completed` regardless of recency, and `attempts` does not exist.

- [ ] **Step 3: Replace the view in migration 004**

Add to the `upgrade()` of `004_run_attempts.py`, after the index creation, a
`CREATE OR REPLACE VIEW executions AS ...` that is the migration 002 definition with three changes:

1. `operation_retried` joins the `event_type` filter in the `WHERE` clause of the `ranked` CTE.
2. The window's `ORDER BY` puts the attempt first, so the latest attempt decides the verdict and the
   severity ranking only breaks ties within it:

```sql
        row_number() OVER (
            PARTITION BY e.run_id, e.component_id
            ORDER BY
                COALESCE((e.data->>'attempt')::int, 1) DESC,
                CASE e.event_type
                    WHEN 'operation_failed' THEN 1
                    WHEN 'operation_canceled' THEN 2
                    WHEN 'operation_completed' THEN 3
                    WHEN 'operation_started' THEN 4
                    WHEN 'operation_skipped' THEN 5
                    WHEN 'operation_retried' THEN 6
                    WHEN 'operation_queued' THEN 7
                END,
                e.timestamp DESC
        ) AS rn,
```

3. A new window carries the attempt count into the outer select:

```sql
        max(COALESCE((e.data->>'attempt')::int, 1)) OVER (
            PARTITION BY e.run_id, e.component_id
        ) AS attempts,
```

selected as `r.attempts`. `operation_retried` must never win the verdict, which is why it sits below
every terminal type in the severity ranking; it is in the view only so it counts toward `attempts`.

Add the matching `DROP` and re-create of the 002 definition to `downgrade()`.

- [ ] **Step 4: Add the column to the read model**

In `packages/interloper-db/src/interloper_db/models/runs.py`, add to `Execution`:

```python
    attempts: int = 1
```

Extend the class docstring: the status is the latest attempt's, and `attempts` is how many that
operation took.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `make dev-reset && uv run pytest packages/interloper-db/tests/store/test_events.py -v`
Expected: PASS.

- [ ] **Step 6: Verify the failed-scope retry still reads correctly**

Run: `uv run pytest packages/interloper-scheduler/tests/test_executor.py -v`
Expected: PASS. `RunExecutor._prior_successes` reads `list_executions` and keys by component id; the
view still returns one row per operation, so it is unaffected. If this fails, the view is returning
more than one row per `(run, operation)` and step 3 is wrong.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-db
git commit -m "feat(db): read an execution's verdict from its latest attempt

By Digitl"
```

---

### Task 6: Hooks observe verdicts

**Files:**
- Modify: `packages/interloper-scheduler/src/interloper_scheduler/hooks.py:87-105` (`_tick`) and `:138-170` (`_event_metadata`)
- Test: `packages/interloper-scheduler/tests/test_hooks.py`

**Interfaces:**
- Consumes: `Run.retry_of`/`Run.root_run_id` (Task 1), `_plan_retry` (Task 2).
- Produces: a sweep that skips a failed run with a successor, and `HookContext.metadata` carrying
  `attempt` and `attempts`. No new hook event types.

- [ ] **Step 1: Write the failing tests**

Add to `packages/interloper-scheduler/tests/test_hooks.py`, following the module's existing way of
building a hook component and a terminal run:

```python
def test_a_failed_run_that_will_be_retried_does_not_fire(controller, store, org, hook, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 2}})
    run = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(run.id, success=False)

    controller._tick()

    assert not fired_events(store, run.id)


def test_an_exhausted_stack_fires_once(controller, store, org, hook, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 1}})
    run = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(run.id, success=False)

    controller._tick()

    assert len(fired_events(store, run.id)) == 1


def test_a_healed_stack_fires_completed_on_the_successful_attempt(controller, store, org, hook, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 2, "delay": 0}})
    first = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(first.id, success=False)
    successor = [r for r in store.runs.list_all(org.id, component_id=job_component.id) if r.id != first.id][0]
    store.runs.complete(successor.id, success=True)

    controller._tick()

    assert not fired_events(store, first.id)
    assert len(fired_events(store, successor.id)) == 1


def test_the_context_carries_the_stack_position(controller, store, org, hook, job_component):
    store.components.merge_config(job_component.id, {"retry": {"max_attempts": 1}})
    run = store.runs.create(org.id, component_id=job_component.id)
    store.runs.complete(run.id, success=False)

    controller._tick()

    assert hook.seen_context.metadata["attempt"] == 1
    assert hook.seen_context.metadata["attempts"] == 1
```

Write `fired_events(store, run_id)` in the test module: the `hook_fired` events attached to that run.
The `hook` fixture is a hook component whose `fire` records the context it was handed; a hook
subscribing to `run_completed` as well as `run_failed` is what makes the third test meaningful.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest packages/interloper-scheduler/tests/test_hooks.py -v -k "retried or exhausted or healed or stack_position"`
Expected: FAIL. The first test fires a notification for an attempt that will be retried.

- [ ] **Step 3: Gate the sweep**

In `packages/interloper-scheduler/src/interloper_scheduler/hooks.py`, add the predicate to `_tick`'s
statement:

```python
            successor = aliased(Run)
            has_successor = select(successor.id).where(col(successor.retry_of) == Run.id).exists()
            runs = session.exec(
                select(Run)
                .where(col(Run.status).in_(_TERMINAL_STATUSES))
                .where(col(Run.completed_at) > since)
                .where(~((Run.status == "failed") & has_successor))
                .order_by(col(Run.completed_at))
            ).all()
```

Import `aliased` from `sqlalchemy.orm`. Extend the module docstring with the rule this implements: a
hook observes a verdict, never an attempt, so a failed run whose next attempt is already queued is
not an outcome and does not fire. Because the successor is created in the same transaction that marks
the run failed, there is no window in which a doomed attempt looks final.

- [ ] **Step 4: Carry the stack position into the context**

In `_event_metadata`, add the stack's position to the dict it builds:

```python
        metadata: dict[str, Any] = {
            "status": run.status,
            "component_name": target.name or target.key,
            "component_key": target.key,
            "attempt": run.attempt,
            "attempts": session.exec(
                select(func.count()).select_from(Run).where(Run.root_run_id == run.root_run_id)
            ).one(),
        }
```

Import `func` from sqlalchemy. Extend the method's docstring: a message addressing humans renders the
stack's position, so it can say the work succeeded on the second attempt or failed after three.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest packages/interloper-scheduler/tests/test_hooks.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full checks**

Run: `uv run ruff check && uv run ty check && uv run pytest`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add packages/interloper-scheduler
git commit -m "feat(scheduler): fire hooks on a stack's verdict, not on each attempt

By Digitl"
```
