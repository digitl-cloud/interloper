# Backfill concurrency

Date: 2026-09-29. Status: approved design, implementation not started.

Scope: make a backfill's concurrency real for the backfills cron creates, declared on the job that
owns them. This is the batch level of the capacity follow-up in `2026-09-17-retry-design.md`,
taken on its own; a limit on a contended resource (a connection, an instance) is not designed
here, and nothing below prevents it.

---

## 1. Problem

A backfill is gated at creation time, not at dispatch. `RunStore.create_backfill` creates the
newest `concurrency` partitions as `queued` and the rest as `pending`; when a run completes,
`_advance_backfill` counts what is still in flight and promotes `pending` runs up to the limit.
The queue worker never reads `concurrency`: it claims whatever is `queued`.

Cron does not call `create_backfill`. It builds its backfill inline, so the row commits in the same
transaction as the job's `next_run_at` advance (a crash between the two would re-create the
backfill next tick, and `create_backfill` opens its own transaction). The inline copy creates every
run `queued` and leaves the row's `concurrency` at the column default of 1, which the UI then shows
as if it were enforced. The choice dates from when a cron firing covered one partition; since
`lookback` counts partitions with an offset, a firing is a real backfill, and the fan-out was never
revisited. A job also declares no concurrency, so a fixed cron would have had nothing to read.

The consequence in production: a job with `lookback: 7` launches seven pods at once against one
vendor, each running up to `max_workers` operations. Retries amplify it, since a burst of failures
becomes a burst of retries one delay later.

A second, smaller gap: `_advance_backfill` counts `queued` and `running` runs as in flight, but a
claimed run is `dispatched` until its pod starts (`RunExecutor._mark_running`). On Kubernetes that
is minutes of scheduling and image pull, during which a completion promotes a `pending` run into a
slot that is not free.

---

## 2. Decisions

| Topic | Decision |
|---|---|
| Where the limit is declared | `CronJob.concurrency: int = 1`, `ge=1`, declared after `offset`: it is a setting of the window the job covers each firing. Section "Operation", so the form shows it with the schedule, `enabled` and `retry`: how the job runs, as opposed to what it covers. |
| What it means | How many partitions of one firing are in flight at once. `1` runs the window serially, newest partition first, matching `Backfill.concurrency`'s default and dispatch order. |
| Where the fan-out lives | One session-level function, `create_backfill_runs(session, db_backfill, window)`, next to `cancel_backfill_runs` in `interloper_db/store/runs.py`. It creates one run per partition, oldest first in creation order, the newest `db_backfill.concurrency` of them `queued` and the rest `pending`, and sets `db_backfill.partitions`. `create_backfill` and cron both call it; the duplicated run loop in `cron.py` goes. |
| What cron stamps | The job's `concurrency` on the backfill row, so what the API and the UI report is what is enforced. Cron keeps building the row inline, in the job's transaction, and keeps skipping the quota checks `create_backfill` applies to user-created backfills. |
| In flight | `queued`, `dispatched` and `running`. `_advance_backfill` counts all three. |
| A retry waiting out its delay | Holds its slot. The slot belongs to the partition (the stack), which is in flight from its first attempt until its last attempt ends, which is how the rest of the system reads stacks. The cost is an idle slot for the delay; promoting past a waiting retry would need a trigger when `scheduled_for` passes, which nothing provides. |
| API-created backfills | Unchanged. `POST /backfills` keeps its own `concurrency` (default 1); a request targeting a job does not read the job's setting, since the request is explicit. |
| Unpartitioned jobs | Unchanged. A firing without a window is a single run and no backfill; `concurrency` is stored but has nothing to gate. |
| `fail_fast` | Out of scope. Cron leaves it `False` as today. |
| Compatibility | Behaviour change, no migration. Cron top-ups are gated at 1 in flight unless the job says otherwise. `Backfill.concurrency` and the JSON config need no schema change. |

---

## 3. What changes

### Core

`interloper/job/cron.py`

```py
concurrency: int = Field(
    default=1,
    ge=1,
    title="Concurrency",
    description="How many partitions of one firing run at once",
    json_schema_extra={"x-section": "Operation"},
)
```

Declared after `offset`. The schema lists the job's own fields first, so the Operation section of
the form reads `cron`, `timezone`, `concurrency`, then the inherited `enabled` and `retry`; the
Partitioning section is unchanged.

### Store

`interloper_db/store/runs.py`

```py
def create_backfill_runs(session: Session, db_backfill: Backfill, window: TimePartitionWindow) -> None:
    """Create a backfill's runs, the newest `concurrency` of them queued and the rest pending."""
```

It reads `org_id`, `component_id` and `concurrency` from the row, creates the rows oldest first
(so the runs list, ordered by `created_at` desc, keeps the newest partition on top), and sets
`db_backfill.partitions = window.partition_count()`. `create_backfill` calls it after adding and
flushing the row; the loop and the `first_queued` arithmetic move out of the method.

`_advance_backfill` counts `("queued", "dispatched", "running")` as in flight.

### Scheduler

`interloper_scheduler/cron.py` builds the row with `concurrency=config.get("concurrency", 1)` and
calls `create_backfill_runs`. The comment explaining that cron queues every partition at once is
replaced by one saying why the row is still built inline: atomicity with the state advance, and no
quota check on top-ups.

### App

No change. The generated form renders the field from its schema, in the Operation section, and the
wizard seeds it from the job's stored config like any other field. The backfills table already
shows `concurrency`; it now shows a true value for cron backfills.

### Docs

- `docs/guide/jobs.md`: `concurrency` in the `CronJob` field table, and a sentence that each firing
  is a backfill gated by it, newest partition first.
- `docs/guide/backfilling.md`: cron top-ups are backfills like any other, gated by the job's
  `concurrency`.
- `docs/ui/index.md`: the job form's Operation section carries `concurrency`.

---

## 4. Tests

Test files mirror the modules they cover.

- `interloper-core/tests/job/test_cron.py`: the field defaults to 1, rejects 0, and sits in the
  Operation section; `tests/job/test_base.py`'s schema-order assertion gains `concurrency` after
  `offset`.
- `interloper-db/tests/store/test_runs.py`: `create_backfill_runs` on a row with `concurrency=2`
  over four partitions queues the newest two and leaves two pending, creation order oldest first;
  the existing `TestCreateBackfill` cases keep passing unchanged. `_advance_backfill` does not
  promote while a run is `dispatched`, and does once it completes.
- `interloper-scheduler/tests/test_cron.py`: a partitioned job with `lookback: 3` and the default
  concurrency creates a backfill whose row says `concurrency == 1`, with the newest partition
  `queued` and the other two `pending`; with `concurrency: 3` all three are `queued` (the existing
  window test, made explicit about why). Completing the queued run promotes the next newest.

---

## 5. Rollout

- Ships as a `feat` with a behaviour-change note: cron backfills now honour a concurrency of 1 by
  default. A job whose firings were meant to run in parallel needs `concurrency` set.
- Production: the eight Swarovski jobs have `lookback: 7` (one has 1). After the deploy, each one
  needs an explicit `concurrency` chosen against its vendor, set the same way their `retry` was.
  Left at the default, each firing runs its seven partitions one at a time, newest first, which
  lengthens the job's wall-clock time by up to seven times.
