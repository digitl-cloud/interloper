# Retry: attempts and verdicts across the execution hierarchy

Date: 2026-09-17. Status: approved design, implementation not started.

Scope: how work that failed is tried again, at every level of the framework that has a unit of work,
and how the outcome of that work is reported once rather than per attempt. This is the first of three
specs derived from one diagnosis (below). It depends on `2026-09-17-operation-is-a-component-design.md`,
which lands first and is what lets a policy be declared once on `Operation`. It covers the policy
primitive, operation-level retry in
the runner, run-level retry in the platform, stacks, verdict-based hooks, and the surfaces that
render them. Capacity (`Limit`/`Limiter`) is a separate spec and is not designed here.

---

## 1. Diagnosis

The framework has an execution hierarchy, batch to run to operation to request. Each level is a unit
of work that can fail, contend for a shared resource, and report an outcome. The levels are equipped
inconsistently:

| level | verdict | attempt | capacity |
|---|---|---|---|
| batch (`Backfill`) | derived, but counts runs | none | `concurrency`, inert for cron backfills |
| run (`Run`) | `status` | manual only, `POST /runs/{id}/retry` | none, the queue drains unbounded |
| operation (`Operation`) | events, via the `executions` view | none | `max_workers`, local and anonymous |
| request | a raised exception | none | none |

That single inconsistency produces the symptoms: retry exists at one level and by hand, contention is
bounded by an anonymous per-process number that cannot name the resource it protects, and hooks
observe attempts rather than verdicts, so they narrate work in progress instead of reporting
outcomes.

The plan is to make the hierarchy uniform: every unit of work has a verdict, an attempt budget, and a
capacity, expressed by the same types at every level. Three specs:

1. **Attempts and verdicts** (this document).
2. **Capacity**: `Limit` declared on the component that is the contended resource, `Limiter` with an
   in-process and a store-backed implementation selected by deployment as `Launcher` already is,
   absorbing `max_workers` and `Backfill.concurrency`.
3. **Request-level retry** in `RESTClient`, which is a small application of this document's policy
   type and a source author's opt-in.

---

## 2. Decisions

| Topic | Decision |
|---|---|
| Primitive | One flat `RetryPolicy`: numbers only, no per-level nesting, no scope. |
| Level binding | The component a policy is declared on fixes the level, because a component's unit is its level. `Operation.retry` governs that operation's execution, `Source.retry` its assets' operations, `Job.retry` that job's runs, `RESTClient(retry=...)` that client's requests. |
| Contract | `retry` is a field on `Operation`, so the runner reads `operation.retry` on any node without narrowing to `Asset`. `Workload` does not declare it: nothing ever reads a run policy off an instance, and `Operation` extends `Workload`, so one name on both contracts would have to mean two levels at once. |
| Multiplication | Nothing multiplies unless two components deliberately declare at two levels. There is no single number reused across levels. |
| Inheritance | An asset inherits its source's policy through `Source._resolve()`, alongside `dataset`, `normalizer` and `materialization_strategy`. No new cascade mechanism. |
| Run-level reach | A run targeting a source or an asset takes the platform default. Run-level retry is a property of the job; a source's `retry` is an operation policy and is never read at run level. |
| Classification | Behaviour, not config: `Operation.retryable(error)`, next to `Operation.failure()`. Exception types never enter serialized config. |
| Automatic scope | An automatic retry is always failed-scope. `scope` stays an argument of the manual retry endpoint. |
| Attempt identity | A run attempt is a `Run` row. A stack is the attempts of one unit of work, keyed by `root_run_id` (its own id for a first attempt). No new table. |
| Successor creation | In `RunStore.complete()`, in the transaction that marks the run failed, before `_advance_backfill`. |
| Dispatch delay | `Run.scheduled_for`, honoured by the queue claim. Status stays `queued`. |
| Backfills | Successors keep their `backfill_id`. `_advance_backfill` finalizes on stacks, not runs. |
| Verdict rule | A hook observes verdicts, never attempts. A failed run that has a successor does not fire. |
| Intermediate failures | `OPERATION_RETRIED` marks an attempt that will be retried. `OPERATION_FAILED` means exhausted. |
| Executions view | Still one row per `(run, operation)`, the verdict. Ranked by attempt first, severity within the latest attempt, plus an `attempts` column. |
| Defaults | None. A policy applies only where a component declares one; there is no instance-wide fallback. Recorded as a follow-up. |
| Compatibility | None required. All consumers ship together. |

---

## 3. The policy

`interloper/retry/base.py`

```py
class RetryPolicy(BaseModel):
    """An attempt budget for one unit of work."""

    max_attempts: int = 3       # total, including the first
    delay: float = 5.0          # seconds before the second attempt
    backoff: float = 2.0        # multiplier per subsequent attempt
    max_delay: float = 3600.0
    jitter: float = 0.1         # fraction of the computed delay, applied symmetrically

    def allows(self, attempt: int) -> bool: ...
    def delay_before(self, attempt: int) -> float: ...
```

`delay_before(attempt)` is `min(delay * backoff ** (attempt - 2), max_delay)`, jittered; attempt 2 is
the first retry. `allows(attempt)` is `attempt <= max_attempts`.

Numbers only. The type is serializable, so it renders in `SchemaForm` without bespoke UI and survives
into a spec unchanged.

### Declaration

Declared once on the unit it governs, which the prerequisite spec makes a real component:

```py
class Operation(Component, Workload):
    retry: RetryPolicy | None = None     # this operation's execution

class Source(Component, Workload):
    retry: RetryPolicy | None = None     # default for its assets' operations

class Job(Component, Workload):
    retry: RetryPolicy | None = None     # this job's runs
```

`Asset` inherits the field from `Operation` and declares nothing. So does `Connection`, where it
governs the renewal operation, which is correct and comes for free.

`@il.asset(retry=...)` and `@il.source(retry=...)` gain the parameter alongside `partitioning=` and
`normalizer=`.

`Source._resolve()` gains one line, in the block that already applies source-level defaults to assets
that do not define their own:

```py
if asset.retry is None and self.retry is not None:
    asset.retry = self.retry
```

### Resolution

One step plus the platform default, per level. There is no walk.

| failing unit | policy |
|---|---|
| request | the policy given to the client at construction; nothing else |
| operation | `operation.retry`, already carrying its source's default for an asset. Nothing else: an operation that declares no policy is attempted once. |
| run | the target's `config.retry` when the target is a job. Nothing else: a run whose target is not a job carrying a policy is attempted once. |

The run-level policy is read live at completion time, so editing a job's policy takes effect on the
next failure rather than on the next run.

### Classification

```py
class Operation:
    def retryable(self, error: Exception) -> bool:
        """Whether another attempt at this operation is worth making."""
        return True
```

Sits next to `failure()`, and is overridden by an operation that can recognise a permanent error. The
default is permissive: the budget, not the classifier, is what bounds waste.

---

## 4. Operation level

The attempt loop is implemented once on `Runner` and called by both execution paths,
`AsyncRunner._execute_operation` and the multi-process worker function, so the policy is applied
identically everywhere. The multi-process runner calls it inside its worker: `retry` is a field on
the operation itself, so it is already serialized with it and needs no separate channel.

The loop wraps the existing try/except in `AsyncRunner._execute_operation`:

- `execute` raises
- the policy is `operation.retry`, so the loop never narrows to `Asset`
- if `operation.retryable(error)` and the policy allows the next attempt: emit `OPERATION_RETRIED`
  carrying the attempt number and the error, sleep `delay_before(next)`, execute again
- otherwise: `state.mark_failed(...)` exactly as today

A retrying node holds its concurrency slot for the duration. The DAG walk, the semaphore and
`fail_fast` are untouched: a node that is still retrying has not failed, so nothing downstream is
cancelled and no fail-fast break is triggered until its budget is exhausted.

Every operation lifecycle event gains `attempt` in its `data`, defaulting to 1 when absent, so
historical rows read correctly.

### Event vocabulary

```py
OPERATION_RETRIED = "operation_retried"
```

`OPERATION_FAILED` keeps its meaning and is emitted only on exhaustion. This is the verdict rule one
level down: an intermediate attempt is recorded, and is not the outcome.

---

## 5. Run level

### Columns

`Run` gains:

- `root_run_id: UUID`, not null, its own id for a first attempt. Stack membership is one indexed
  predicate with no `COALESCE`.
- `scheduled_for: datetime | None`, the earliest instant the run may be claimed.

`retry_of`, `attempt` and `retry_scope` already exist and keep their meaning. Run ids are assigned in
Python (`uuid4()`) at every creation site so `root_run_id` can always be set; the server default
stays as a safety net.

### The terminal path

`RunStore.complete()` is the single terminal path every failure takes: the executor's verdict, the
executor's exception handler, the queue's launch failure, and the reaper's timeout. Planning the
successor there, rather than in a sweeping controller, is what makes the hook rule race-free: "has a
successor" becomes true in the same transaction that makes the run failed, so there is no window in
which a doomed attempt looks final.

```py
def complete(self, run_id, *, success):
    ...
    db_run.status = "success" if success else "failed"
    db_run.completed_at = now
    UsageLedger(session).settle_run(db_run, success=success)
    ... stamp component state ...
    if not success:
        self._plan_retry(session, db_run)      # flushes, so the successor is visible below
    if db_run.backfill_id:
        self._advance_backfill(session, db_run.backfill_id, failed=not success)
    commit(session)
```

`_plan_retry` reads the target component row (`config` is plain JSON, so this is one row read and no
hydration), resolves the policy, and inserts the successor when the policy allows the next attempt
and the target still exists:

```py
Run(
    id=uuid4(),
    org_id=db_run.org_id,
    component_id=db_run.component_id,
    backfill_id=db_run.backfill_id,
    partition_key=db_run.partition_key,
    status="queued",
    scheduled_for=now + policy.delay_before(db_run.attempt + 1),
    retry_of=db_run.id,
    root_run_id=db_run.root_run_id,
    attempt=db_run.attempt + 1,
    retry_scope="failed",
    billable=db_run.billable,
)
```

Ordering matters: the successor must exist before `_advance_backfill` runs, so the in-flight count
sees it and the backfill is not finalized prematurely.

A run whose component was deleted (`component_id` is null) is not retried. A canceled run is not
retried: cancellation does not go through this path. The quota is not checked at creation; dispatch
is the authoritative gate and cancels an over-quota run at claim time, as it does for any run.

### Dispatch

`QueueController._claim_next` gains one predicate:

```sql
WHERE status = 'queued' AND (scheduled_for IS NULL OR scheduled_for <= now())
ORDER BY created_at ASC
```

Status stays `queued`, so nothing else in the pipeline changes, and a delayed run is simply not yet
claimable.

### Failed scope

`RunExecutor._prior_successes` already walks the `retry_of` chain and marks previously successful
operations non-materializable, keyed by component row id. It is unchanged: the executions view keeps
one row per `(run, operation)` carrying the latest attempt's verdict, which is exactly what the walk
reads.

---

## 6. Backfills

A cron `lookback` window is a backfill, so retries have to live inside backfill accounting. This is
the part the earlier manual-retry work deliberately punted, and it cannot be punted.

- The successor keeps its `backfill_id`. A retry belongs to the batch that produced it.
- `_advance_backfill` finalizes on stacks: the batch is complete when no stack has an attempt in
  flight or pending, and it failed only if some stack's latest attempt failed. Attempt 1 dying no
  longer condemns a backfill that attempt 2 healed.
- `Backfill.partitions` is unchanged: it counts partitions, and there is still exactly one stack per
  partition.

Progress counts stacks, so a backfill of 7 partitions reports out of 7 however many attempts it took.

---

## 7. Verdicts

### The executions view

Today the view ranks `operation_failed` above `operation_completed` regardless of recency, so an
operation that failed and then healed would read as failed. It keeps one row per `(run, operation)`,
the verdict, and changes its ordering:

```sql
row_number() OVER (
    PARTITION BY e.run_id, e.component_id
    ORDER BY
        COALESCE((e.data->>'attempt')::int, 1) DESC,
        CASE e.event_type ... END,
        e.timestamp DESC
)
```

plus an `attempts` column, `max(attempt)` over the same partition. `operation_retried` is added to
the view's event filter so it counts toward `attempts` without ever winning the verdict.

Every consumer keeps working unchanged, `_prior_successes` included. Per-attempt detail stays in
`events`, which is where detail belongs.

### Hooks

`HookController` gains one predicate in its sweep, so a failed run with a successor is never
evaluated:

```sql
AND NOT (runs.status = 'failed' AND EXISTS (
    SELECT 1 FROM runs successor WHERE successor.retry_of = runs.id
))
```

No knowledge of budgets, backoff or policy, and no new hook event types: `run_completed` and
`run_failed` keep their names and now mean the stack's verdict. A stack that succeeds on attempt 3
fires `run_completed` once, on that attempt. A stack that exhausts its budget fires `run_failed`
once, on the last attempt.

`HookContext.metadata` gains `attempt` and `attempts` (the stack's total), so a message can say
"succeeded on attempt 2" or "failed after 3 attempts". The firing claim stays `uuid5(hook, run)`:
attempts that do not fire never claim, so no double firing is possible.

---

## 8. Persistence and migration

Migration 004 (the chain was folded to 001-003):

1. `ALTER TABLE runs ADD COLUMN root_run_id uuid, ADD COLUMN scheduled_for timestamptz`
2. backfill existing rows:

```sql
WITH RECURSIVE chain AS (
    SELECT id, id AS root FROM runs WHERE retry_of IS NULL
    UNION ALL
    SELECT r.id, c.root FROM runs r JOIN chain c ON r.retry_of = c.id
)
UPDATE runs SET root_run_id = chain.root FROM chain WHERE runs.id = chain.id;
```

3. `ALTER TABLE runs ALTER COLUMN root_run_id SET NOT NULL`
4. `CREATE INDEX ix_runs_root_run_id ON runs (root_run_id)`
5. an index supporting the claim predicate: `(status, scheduled_for, created_at)`
6. `CREATE OR REPLACE VIEW executions` with the attempt-aware ranking

Any run row whose `retry_of` points at a deleted run (the FK is `ON DELETE SET NULL`) becomes its own
root, which is correct: its lineage is gone.

---

## 9. Surfaces

### API

- `RunResponse` gains `root_run_id`, `scheduled_for`, and `attempts` (the stack's total).
- `GET /runs` becomes stack-native: one row per stack, the latest attempt, selected with
  `DISTINCT ON (root_run_id) ... ORDER BY root_run_id, attempt DESC` and re-ordered by `created_at
  DESC` in an outer query so the listing order is unchanged. Filters and counts apply to the stack's
  verdict, which is what a reader means by "show me failed runs".
- `GET /runs?root_run_id=...` lists the attempts of one stack.
- `POST /runs/{id}/retry` is unchanged and keeps its explicit `scope`.

### App

- Runs table: one row per stack, the latest attempt's status, an `n/N` chip when `attempts > 1`,
  expandable to the attempts. Watch the known `UTable` traps: an inline `:grouping` literal with
  `v-model:expanded` auto-resets, and `resolveComponent` returns a bare string inside header and cell
  render functions.
- Run detail: attempt navigation across the stack, and the failed-scope attempts showing which
  operations were carried forward as already successful.
- Timeline: an operation with `attempts > 1` reads as its verdict, with its retried attempts visible
  from the events it emitted.
- Retry policy needs no bespoke UI: it is a config field on a component, so `SchemaForm` renders it.

---

## 10. No instance-wide default

Deliberately none. A policy is in force only where a component declares one, so retry is visible at
the thing it retries and nothing acquires attempts by deployment. A deployment-wide default is a real
want, and it is recorded as a follow-up rather than guessed at now: it needs its own answer to how an
instance default interacts with a declared one, and to whether operation and run defaults belong in
one block or with the runner that owns execution.

The consequence to keep in mind while the two land: nothing retries until a job or an asset says so,
so phases 1 and 2 change no behaviour on their own.

---

## 11. Request level

Out of scope here beyond the type it shares. The third spec gives `RESTClient` a `retry:
RetryPolicy | None` constructor argument, honoured around the transport call together with
`Retry-After` when the response carries one. A source author opts in where the client is built,
typically the source's `client` cached property, and is free to ignore it and handle vendor
semantics directly. Nothing resolves a request policy from a component.

---

## 12. Testing

Tests mirror the package layout one to one; no standalone feature files.

- `interloper-core`: `tests/retry/test_base.py` for the budget and backoff maths, including jitter
  bounds and `max_delay` capping. `tests/runner/test_async_runner.py` for the attempt loop: a node
  that heals on attempt 2 reports success and emits `OPERATION_RETRIED` then `OPERATION_COMPLETED`; a
  node that exhausts its budget emits `OPERATION_FAILED` once; a node whose `retryable()` declines is
  not retried; a retrying node does not trigger a fail-fast break. `tests/source/test_base.py` for
  the `_resolve` inheritance.
- `interloper-db`: `tests/store/test_runs.py` for successor creation, exhaustion, a deleted target,
  the `scheduled_for` stamp, and the claim predicate; stack-based backfill finalization, in
  particular a backfill whose attempt 1 failed and attempt 2 succeeded reporting success.
- `interloper-scheduler`: `tests/test_hooks.py` for the gate: a failed run with a successor does not
  fire, an exhausted one fires `run_failed` once, a healed stack fires `run_completed` once on the
  successful attempt.

---

## 13. Phasing

1. **Core**: `RetryPolicy`, `Operation.retryable`, the fields on `Operation`/`Source`/`Job`, the
   `_resolve` inheritance, the decorator parameters, `OPERATION_RETRIED`, the runner attempt loop.
2. **Platform**: the two columns, migration 004, the executions view, `_plan_retry` in `complete()`,
   the claim predicate, stack-based backfill accounting, the hook gate.
3. **Surfaces**: API fields and the stack-native runs listing, then the app.

Each phase is independently shippable: after phase 1 operations retry in place with nothing else
changed, after phase 2 runs retry and hooks report verdicts, after phase 3 the stack is legible.

---

## 14. Follow-ups, recorded

- **An instance-wide default.** Dropped from this design on purpose. Whoever picks it up owns two
  questions: whether an instance default is a floor, a ceiling or a plain fallback under a declared
  policy, and whether the operation-level default belongs in a settings block next to the run one or
  on the runner that executes operations, where `max_workers` and `fail_fast` already live.
- **Capacity spec**: `Limit` and `Limiter`, absorbing `max_workers` and `Backfill.concurrency`, with
  a store-backed implementation so a limit can be shared across pods. This is the tuning surface for
  vendor quotas, and the reason retry is not being asked to solve contention.
- **Cron backfill concurrency**: cron builds its backfill inline and queues every partition at once,
  so `Backfill.concurrency` is recorded but inert for exactly the backfills that dominate production.
  Folded into the capacity spec rather than fixed on its own.
- **Backfill-level hook events**: stack gating stops a healing attempt from notifying, but a
  genuinely broken source still notifies once per partition. `backfill_completed` / `backfill_failed`
  would collapse a cron firing into one message. Deliberately deferred until the noise left after
  stack gating is known.
- **Run-level classification**: the run level has no exception object, so it retries everything
  within its budget. If a class of run failure proves never worth retrying, it needs a verdict
  richer than a boolean, which is a change to the terminal path's signature.
