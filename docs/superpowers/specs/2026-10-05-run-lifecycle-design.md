# Run lifecycle owned by the store

Step 2c of the interloper-db / interloper-api simplification, after the insights facet (#426).

## Problem

The scheduler wrote run rows itself, through `Session(store.engine)` and `store.transaction()` sessions, at nine sites:
- the queue's claim, quota reservation and over-quota cancel;
- the executor's start and its retry-chain walk;
- the reaper's dispatched scan and failure event;
- renewal's due scan and its run insert;
- the hooks sweep's queries;
- cron's due-job lock.

Run statuses were bare strings, the terminal set was defined three times, and the launcher's pod-state enum was also called `RunStatus`.

## Decisions

1. **Boundary.** Raw database access lives in interloper-db alone. The scheduler, api, toolkit, mcp and agent packages ban `sqlalchemy`, `sqlmodel` and `interloper_db.session` through ruff's `banned-api` (`TID251`); their tests are exempt, since they seed rows directly. The scheduler's only database handles are the store facets and `with store.transaction():`, whose session it never touches.
2. **Status vocabulary** in `interloper_db.models`:
   - `RunStatus` (`pending`, `queued`, `dispatched`, `running`, `success`, `failed`, `canceled`) and `BackfillStatus` (`queued`, `running`, `success`, `failed`, `canceled`), both `str` enums. The columns stay `VARCHAR`, so there is no schema change.
   - `TERMINAL_RUN_STATUSES`, `OPEN_RUN_STATUSES` and `ACTIVE_BACKFILL_STATUSES` are the one definition of each set.
   - The launcher's pod-state enum becomes `LaunchStatus`, and its dataclass `LaunchState`.
   - Execution statuses (the `executions` view), core's `ExecutionStatus`, and the Slack hook's labels are separate vocabularies and stay as they are. The Slack hook depends only on interloper-core.
3. **One verb where the operation is row mechanics; an ambient transaction where the scheduler decides.**
   - `runs.claim_next()` locks the oldest claimable queued run, reserves its quota, and dispatches it; or it cancels the run (with its backfill) and its reason, then tries the next.
   - `runs.start(run_id)` marks a run running and stamps `started_at`; a terminal run raises `ConflictError`.
   - `runs.fail(run_id, error, *, metadata=None)` records the `run_failed` event and the verdict in one transaction. A terminal run raises `ConflictError` and records nothing.
   - `components.lock_due(kind, state_key, *, now, limit, keys=None, enabled_only=False)` locks rows whose state instant has come, most overdue first and never-scheduled last. Cron and renewal call it inside `store.transaction()`, then stamp state and create runs through the facets.
4. **Hooks pending.**
   - `RunQuery(hooks_pending=True)` and `BackfillQuery(hooks_pending=True)` list terminal rows whose `hooks_evaluated_at` is unset. `runs.mark_hooks_evaluated` and `backfills.mark_hooks_evaluated` stamp them.
   - The store stamps a run at the transition that makes its verdict moot: cancelation (backfill cancel, fail-fast, quota denial) and a planned retry (automatic or manual). So the sweep reads exactly the verdicts hooks still owe and has a single loop per subject.
   - Migration 010 stamps the rows such transitions reached before this release.
5. **Small surface changes:**
   - `RunQuery.status` and `BackfillQuery.status` take a list of members; a single `?status=failed` still parses.
   - `RelationQuery.dst_id` filters by destination.
   - `runs.list` and `backfills.list` accept `org_id=None` for in-process sweeps; HTTP routes always pass the caller's organisation.
   - `components.job_partition_granularity` loses its `session` parameter.
   - `cancel_backfill_runs` becomes the private `BackfillStore._cancel`.

## Behaviour changes

- A run whose target was deleted is failed with that reason when the executor picks it up. Before, it stayed dispatched until the reaper's timeout.
- A run another writer finished first is no longer flipped back to `running` by a late executor start.
- A failure reason that cannot be written now leaves the run dispatched for the reaper's next tick. Before, the run was failed without its reason.
- `lock_due` orders never-scheduled rows last for renewal too. Before, renewal took them first.
- `GET /runs` accepts `hooks_pending`, because every `RunQuery` field is an HTTP filter.

## Size

Source net +162 lines, tests +76; the estimate was about −80.
- The scheduler lost 262 lines and no longer imports SQL.
- The store gained the verbs (`runs.py` +180), the enums (+34), `lock_due` (+46) and the backfill cancel paths (+53).
- Fixed costs are migration 010 (+39) and the ruff ban in five `pyproject.toml` files (+35).
