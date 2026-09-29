# Backfill hook events

Date: 2026-09-29. Status: approved design, implementation not started.

Scope: hooks can observe a backfill's verdict, not only a run's, and the evaluator delivers every
verdict it is owed. This closes the last item of the retry design's original ask (one
notification per outcome, not per attempt) now that every firing of a partitioned cron job is a
backfill (`2026-09-29-backfill-concurrency-design.md`).

---

## 1. Problem

A hook reacts to `run_completed` and `run_failed`. Since a cron job's firing became a backfill
gated by `concurrency`, a broken vendor makes a job with `lookback: 7` produce seven failed runs,
each a verdict of its own (stack gating only holds back attempts), so a hook watching the job
posts seven messages for one bad morning and nothing that says the firing as a whole failed.

The batch already has a verdict: `_advance_backfill` finalises a backfill as `success` or `failed`
from each partition's latest attempt, and keeps it open while a retry waits out its backoff. No
hook can subscribe to it, and no event announces it: `EventType.BACKFILL_COMPLETED` and
`BACKFILL_FAILED` exist and are emitted nowhere.

Two delivery gaps sit beside this, in the sweep the batch events would inherit:

- The evaluator sweeps by a wall-clock watermark from the scheduler's clock against a
  `completed_at` stamped by the completing process, with a fixed 30 s overlap. A pod whose clock
  trails the scheduler's by more than that, or a `complete()` transaction that takes longer than
  that to commit, produces a verdict the sweep never sees.
- The watermark starts at boot, so a verdict reached while the scheduler was restarting (every
  rollout) is never delivered.

And one race unrelated to retries: `RunStore.complete()` does not check that the run is still
non-terminal, so the reaper failing a run whose pod started at the 600 s mark can overwrite the
executor's success, and now also queue a retry of a run that succeeded.

---

## 2. Decisions

| Topic | Decision |
|---|---|
| Vocabulary | `HookEvent` gains `backfill_completed` and `backfill_failed`. A hook picks the levels it wants; the default stays `["run_failed"]`. A hook subscribed to both levels gets a message per partition and one for the batch, which is what it asked for. |
| What a backfill event is | The backfill reaching `success` or `failed`. A canceled backfill (a user's cancel, or the quota) is not an outcome of the work and fires nothing. A single-partition backfill (a manual partition run) is a backfill like any other. |
| Sweep | The evaluator sweeps backfills beside runs, same tick, same matching, same claim mechanism. Run events are unchanged: a run inside a backfill still fires run events. A failed run whose successor is queued is not a verdict: it is stamped as evaluated without firing, so it leaves the unevaluated set like any other row. |
| Matching | Hooks watching the backfill's target, or the target's parent, as for runs. |
| Claim | `uuid5(hook, backfill_id)`, an `events` row with no `run_id`; `data` carries `backfill_id`. |
| Context | `HookContext` gains `backfill_id`, `start_key` and `end_key`; `run_id` and `partition_key` stay for run events and are `None` for backfill events. |
| Metadata | `status`, `component_name`, `component_key`, `partitions`, `counts` (per status, each partition read as its latest attempt, from `RunStore.count_backfill_runs`), and for a failure `failed_partitions`: the failed partitions' keys with each one's recorded error, newest partition first. |
| Slack | A headline per outcome (`backfill completed` / `backfill failed`), a details line with the range and the counts, then the failed partitions with their errors, at most ten, then "and N more". |
| Webhook | The payload gains `backfill_id`, `start_key` and `end_key`; `metadata` carries the rest. |
| Trigger hooks | On a backfill event the trigger creates a backfill over the same range on each target, so a cascade stays aligned with what fired it. Its concurrency is the target job's `concurrency` when the target is a job, else 1. The re-entry guard is unchanged. |
| Delivery cursor | The watermark and its overlap go. Runs and backfills gain `hooks_evaluated_at`; the sweep takes terminal rows where it is null, ordered by `completed_at`, and stamps it once every matching hook has been evaluated. No clock is compared with another, and a verdict reached during a restart is delivered after it. The idempotent claim stays as the guard against a stamp lost to a crash mid-tick. |
| First deploy | The migration stamps every existing terminal run and backfill, so history is not replayed. |
| Terminal guard | `complete()` raises `ValueError` when the run is already terminal. The reaper already logs and moves on; the executor logs it too, since a run the reaper failed first is not its to finish. |
| Compatibility | Additive for hooks: stored `events` values keep their meaning. One migration (006): two nullable columns with partial indexes on the unevaluated rows, plus the stamp. |

---

## 3. What changes

### Core

`interloper/hook/base.py`

```py
HookEvent = Literal["run_completed", "run_failed", "backfill_completed", "backfill_failed"]

class HookContext(BaseModel):
    event_type: str
    component_id: str
    run_id: str | None = None
    partition_key: str | None = None
    backfill_id: str | None = None
    start_key: str | None = None
    end_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    trigger: Callable[[str], None] | None = Field(default=None, exclude=True)
```

The `events` field description becomes "Outcomes this hook reacts to". The form's multi-select
picks the new values up from the schema.

`interloper/hook/webhook.py`: `_payload` adds `backfill_id`, `start_key`, `end_key`.

`interloper_slack/hook.py`: `_OUTCOMES` gains the two backfill entries; `_details` renders the range
and the counts for a backfill; a failure appends the failed partitions.

### Store

`interloper_db/models/runs.py`: `hooks_evaluated_at: datetime | None` on `Run` and `Backfill`, with
a partial index each on `(completed_at) WHERE hooks_evaluated_at IS NULL`.

`interloper_db/migrations/versions/006_hook_cursors.py`: the columns, the indexes (`CONCURRENTLY`,
`IF NOT EXISTS`, as 005), and `UPDATE ... SET hooks_evaluated_at = completed_at WHERE status IN
('success', 'failed', 'canceled') AND hooks_evaluated_at IS NULL` on both tables.

`interloper_db/store/runs.py`

- `complete()` raises `ValueError(f"Run {run_id} is already {status}")` when the row's status is
  `success`, `failed` or `canceled`.
- `RunStore.failed_partitions(backfill_id) -> list[tuple[str, str | None]]`: the partitions whose
  latest attempt failed, newest first, each with the error of that attempt's `run_failed` event.
- `RunStore.create_backfill` is unchanged; the trigger calls it.

### Scheduler

`interloper_scheduler/hooks.py`

- `_tick` selects runs and backfills with a terminal status and `hooks_evaluated_at IS NULL`,
  ordered by `completed_at`. A failed run with a successor is stamped and skipped; every other row
  is stamped once its hooks have been evaluated. The claims themselves are written as today, one
  `events` row each, so a crash between a claim and its stamp re-evaluates the row and finds the
  claim.
- `_evaluate` is split by subject: the run path is today's; the backfill path reads the target,
  matches the same hooks, builds the backfill metadata, and fires with a backfill context.
- `_trigger` takes the context's shape into account: a run event creates a run on the target with
  the partition propagated (today), a backfill event creates a backfill over `start_key` to
  `end_key`.
- `_OVERLAP`, `_watermark` and the first-tick logic go, with `TestFirstTickWatermark`.

`interloper_scheduler/executor.py`: the terminal `complete()` calls catch `ValueError`, log it at
warning level with the run id, and return `False`.

### Docs

- `docs/guide/hooks.md`: the two new events, what a backfill verdict is, the trigger's backfill
  cascade, and that a canceled backfill fires nothing.
- `docs/ui/index.md`: the hook form's events.

---

## 4. Tests

Test files mirror the modules they cover.

- `interloper-core/tests/hook/test_base.py`: the vocabulary lists four events; the context's
  backfill fields default to `None`.
- `interloper-core/tests/hook/test_webhook.py`: the payload carries the backfill fields.
- `interloper-slack/tests/test_hook.py`: a backfill failure renders the range, the counts and the
  failed partitions, capped at ten; a completion renders no error block.
- `interloper-db/tests/store/test_runs.py`: `complete()` refuses a terminal run; `failed_partitions`
  reads each stack's latest attempt and its error, newest first.
- `interloper-db/tests/migrations/`: 006 stamps existing terminal rows and leaves open ones unstamped.
- `interloper-scheduler/tests/test_hooks.py`: a finished backfill fires a hook subscribed to
  `backfill_failed` once, with the counts and failed partitions in its metadata; its runs still fire
  run events for a hook subscribed to those; a backfill kept open by a waiting retry fires nothing
  until it closes; a canceled backfill fires nothing; a terminal row is stamped after evaluation and
  not selected again; a failed run with a queued successor is stamped without firing; a row completed before the controller started is still evaluated; a trigger
  hook on a backfill event creates a backfill over the same range with the target job's
  concurrency.
- `interloper-scheduler/tests/test_executor.py`: a run the reaper already failed is logged, not
  re-completed.

---

## 5. Rollout

- Ships as a `feat` with migration 006. The migration's stamp keeps the first sweep from replaying
  history; after that, restarts deliver late rather than never.
- The Swarovski Slack hooks should subscribe to `backfill_failed` and drop `run_failed`, so a bad
  morning is one message naming the failed partitions. Set the same way their jobs' `retry` and
  `concurrency` were.
