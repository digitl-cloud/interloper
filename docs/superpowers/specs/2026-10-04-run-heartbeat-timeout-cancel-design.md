# Run heartbeat, timeout and cancel

Date: 2026-10-04, rebased on the store-owned run lifecycle (`2026-10-05-run-lifecycle-design.md`)
on 2026-10-05. Status: implemented on `refactor/run-heartbeat-timeout`.

Scope: know when a run is dead, bound how long a run may take, and let an operator stop one. A
policy for overlapping runs of the same job (allow, skip, queue) is not designed here.

---

## 1. Problem

Run `ac209277` (job "Amazon SP", backfill `bfc9f702`, partition 2026-09-29) showed `running` for
27 hours. GKE upgraded the `default` node pool on 2026-10-03 between 06:27 and 06:42 UTC; the run
pod was killed in the drain, its last event is at 06:38:18, and the Kubernetes Job was deleted
300s later by `ttlSecondsAfterFinished`. Nothing marked the run terminal, so its backfill
(`concurrency: 1`) left partitions 09-26 to 09-28 `pending` indefinitely, and its quota
reservation was never released.

Gaps:

- The reaper scans `runs.dispatched()` only. A run that dies after `runs.start` stays `running`
  forever, cannot be retried (retry needs `failed`) and cannot be cancelled.
- Liveness is inferred from the launcher (`describe_run` returning a `LaunchState`). The Kubernetes
  launcher maps every API error to `LaunchStatus.NOT_FOUND`, the Job disappears after its TTL, and
  the in-process launcher has no introspection at all.
- The reaper's timeout counts from `created_at`, so a backfill run that waited hours in `pending`
  can be reaped the moment it is dispatched (the reason the chart overrides `reaper.timeout` to
  3600).
- No run has a deadline, and 16 of 20 connector `RESTClient`s have no HTTP timeout, so a hung
  call runs forever.
- No single run can be cancelled. `backfills.cancel` cancels `pending` and `queued` runs only;
  in-flight runs "drain on their own".

Already fixed on main by the store-owned lifecycle: a late `runs.start` no longer revives a run
another writer finished.

---

## 2. Decisions

| Topic | Decision |
|---|---|
| Liveness | The executor records a heartbeat (`runs.heartbeat_at`); silence past a threshold means dead. The launcher no longer decides liveness. |
| Authority | The run row is the only authority, and the store owns every transition (row lock, then the expected status checked). This design adds the transitions below; the scheduler still issues no SQL (ruff `TID251`). |
| Stopping | One channel for every end: something finishes the row; a live pod sees it on its next heartbeat and exits the process. A dead pod needs nothing. |
| Timeout | `Job.timeout` (seconds, `None` = instance default); instance `run_timeout` default 12h (`None` disables). Derived at sweep time from `started_at`, not stored. |
| Timeout outcome | `failed` with a "timed out" error; the job's retry policy and failure hooks apply. No new status. |
| Cancel | `canceled`, from any open status, never retried, fires no hooks. API, toolkit/MCP and UI. Backfill cancel also cancels in-flight runs. |
| Diagnostics | Optional `Launcher.diagnose(run_id) -> str \| None`, used only to enrich a "lost" or "never started" error. It never decides anything. |
| Storage | One column, `runs.heartbeat_at`. No new table. The `runs` notify trigger skips heartbeat-only updates. |
| Out of scope | Overlap policy; SIGTERM handling (a drain is caught by the heartbeat timeout like any death). |

---

## 3. Run lifecycle

`RunStatus` is unchanged: `pending -> queued -> dispatched -> running -> success | failed | canceled`.

Store verbs, each locking the row (`_lock`) and checking the status it expects:

| Verb | Writer | From | New in this design |
|---|---|---|---|
| `runs.claim_next()` | queue worker | `queued` | also stamps `heartbeat_at` |
| `runs.start(run_id)` | executor | `dispatched` | requires `dispatched` (today: not terminal); also stamps `heartbeat_at` |
| `runs.heartbeat(run_id) -> bool` | executor heartbeat | `running` | new |
| `runs.complete` / `runs.fail` | executor | not terminal | unchanged |
| `runs.overdue(...)` | reaper | | new named read, replaces `runs.dispatched()` |
| `runs.reap(run_id, error, *, heartbeat_at)` | reaper | open, heartbeat unchanged | new |
| `runs.cancel(run_id)` | API, toolkit | `pending`, `queued`, `dispatched`, `running` | new |

`start` refuses anything but `dispatched`, so a run cancelled or reaped before its pod came up is
never executed: the executor exits on the `ConflictError` it already handles.

### Finishing step

`complete`'s body becomes a private `_finish(db_run, status)`, shared by `complete`, `fail`,
`reap` and `cancel`. In the transaction that already holds the row it sets the status and
`completed_at`, settles quota (a cancel settles as a non-success, releasing the reservation),
stamps the target's last run on a verdict (a cancel does not, like the existing cancel paths),
plans the retry (`failed` only), advances the backfill, stamps `hooks_evaluated_at` on a cancel
(`Run.cancel()`), and closes the run's open executions (`EventStore.close_operations`): each
operation's latest attempt without a verdict gets `operation_failed` carrying the reason when it
had started, `operation_canceled` otherwise or on a cancel. The ids come from
`RunState.operation_event_id` (made public), so a late event from the pod deduplicates. The
derivation reads the run's events directly rather than the `executions` view, which is
Postgres-only. A cancel also records a `Run canceled` warning log event.

---

## 4. Heartbeat (executor)

A `RunHeartbeat` thread starts after `runs.start` and stops when `execute` returns. It is a thread
rather than an asyncio task so an operation that blocks the event loop cannot silence it.

```
every heartbeat_interval:
    store.runs.heartbeat(run_id)
      UPDATE runs SET heartbeat_at = now() WHERE id = :id AND status = 'running'
    True                                    -> continue
    False                                   -> finished elsewhere: exit the process
    DB errors for over heartbeat_timeout/2  -> cannot prove liveness: exit the process
```

- `runs.heartbeat` is a single conditional update, not a locking read: it must not wait behind a
  transaction holding the row.
- Exiting is `os._exit(1)` in a container: `RunExecutor(reaper=..., on_lost=...)` takes it, and
  the `interloper launch` entrypoint passes its process exit. The callback is injected, so tests
  run without killing the process.
- The in-process launcher shares the scheduler process and cannot exit it: the run is finished
  correctly in the DB, and a thread blocked in sync code lingers until it returns. Acceptable for
  the dev launcher.
- The error window is measured from when the last successful update was sent, which precedes its
  commit, so the pod's clock never runs behind the database's view of the same heartbeat.
- Exiting at half the heartbeat timeout guarantees a pod cut off from the DB is gone before the
  sweep can fail its run and a retry can start writing the same partitions.
- `Launcher.from_settings` takes the `reaper` settings beside `runner`. The Kubernetes and Docker
  launchers forward `heartbeat_interval` and `heartbeat_timeout` into the run's environment
  (`INTERLOPER_REAPER_HEARTBEAT_*`), the same way they forward the runner settings.

---

## 5. Sweep (reaper)

The `Reaper` keeps its name, its place in the scheduler singleton, its loop and its hourly usage
reconciliation. `_reap` asks the store one named question and acts on the answer.

`runs.overdue(*, startup_timeout, heartbeat_timeout, run_timeout)` returns every organisation's
dispatched and running runs matching one of three rules, each with its failure reason. The rules
are `Run.overdue(now, ...)` on the model, judged in Python against the database clock
(`session.database_now`, shared with quota metering): the set of open runs is small, and the
arithmetic stays portable to the SQLite test dialect.

| Rule | Condition | Error |
|---|---|---|
| Never started | `dispatched` and `heartbeat_at < now() - startup_timeout` | `Run did not start within {startup_timeout}s` + diagnosis |
| Lost | `running` and `heartbeat_at < now() - heartbeat_timeout` | `No heartbeat since {heartbeat_at}` + diagnosis |
| Timed out | `running` and `started_at + timeout < now()` | `Timed out after {timeout}s` |

- `timeout` is `components.config->>'timeout'` of the run's job, else the instance `run_timeout`;
  a run whose job is gone uses the instance default. `run_timeout = None` disables the rule.
- Heartbeats are written with the database's `now()`, and the rules read the same clock.
- For each match the reaper calls `launcher.diagnose(run_id)`, appending its string in
  parentheses; an exception or `None` leaves the plain message. A timed-out run's workload is
  still running, so its diagnosis is `None`. Then it calls
  `runs.reap(run_id, error, heartbeat_at=<as read>)`.
- `runs.reap` locks the row and fails the run (the `run_failed` event and `_finish`, as `fail`
  does) only if it is still open and its `heartbeat_at` is the one read. A pod that started or beat
  between the read and the lock changed it, so the run is left alone and `reap` returns `None`.
  Diagnosis runs outside the lock because it calls the launcher.
- The Kubernetes Job is deleted 300s after failing; the sweep reaches a lost run within about
  `heartbeat_timeout + poll_interval` (~105s), well inside that window.

---

## 6. Cancel

- `runs.cancel(run_id)`: `_finish` with `canceled`; `ConflictError` when the run is already
  terminal. Cancelling one run of a backfill cancels that run only; the backfill advances.
- API: `POST /runs/{run_id}/cancel`, editor role, 409 on `ConflictError` (the API's existing store
  error mapping).
- Toolkit/MCP: `cancel_run`, beside `retry_run`, with `Effect.CANCEL`.
- UI: Cancel button with confirmation on the run page while the run is open, in the toolbar above
  Retry. Like Retry and the backfill Cancel, it is not gated client-side: the API answers a viewer
  with 403.
- `backfills.cancel(backfill_id, *, in_flight=True)` also cancels `dispatched` and `running` runs,
  through `runs.cancel` (they hold quota reservations), after the backfill is terminal so their
  endings do not advance it. `BackfillStore` reads the runs facet through a lazy provider, the
  pattern `QuotaStore` already uses for its defaults, since the runs facet is built after it. A
  quota denial passes `in_flight=False`: the runs in flight already hold their reservations and
  drain as before. The backfill page and the toolkit copy follow.
- A live pod exits on its next heartbeat (within `heartbeat_interval`).

---

## 7. Data model and settings

Migration 011:

- `runs.heartbeat_at timestamptz NULL`, not indexed (heartbeat updates stay heap-only).
- `UPDATE runs SET heartbeat_at = now() WHERE status = 'dispatched'`, so runs dispatched before the
  upgrade get the startup rule.
- `trg_runs_notify` split into an `INSERT` trigger and an `UPDATE` trigger guarded by
  `WHEN (OLD.heartbeat_at IS NOT DISTINCT FROM NEW.heartbeat_at OR OLD.status IS DISTINCT FROM NEW.status)`,
  both on `notify_target_change` (`OLD` is not allowed in an `INSERT` trigger's `WHEN`).

Runs already `running` at upgrade have `heartbeat_at = NULL`; `NULL < x` is false, so only the
timeout rule applies to them. Old-version pods in flight are never failed for heartbeats they
cannot send, and the current ghost run is finished on the first sweep after deploy.

`Job.timeout: int | None = None` (seconds, x-section "Operation", beside `retry`).

`ReaperSettings`:

| Field | Default | Note |
|---|---|---|
| `poll_interval` | 15 | was 60 |
| `startup_timeout` | 600 | renamed from `timeout`; measured from dispatch |
| `heartbeat_interval` | 10 | forwarded to run pods |
| `heartbeat_timeout` | 90 | |
| `run_timeout` | 43200 | `None` disables |

The chart's `extraConfig.reaper.timeout: 3600` override is removed. Settings forbid unknown keys,
so a deployment still carrying `reaper.timeout` (a gitops override) fails at startup: the key must
leave every override with this release. The admin config snapshot (`AdminReaperConfig`, the
admin config page) shows the new fields.

---

## 8. Removals

- `Launcher.describe_run`, `LaunchStatus`, `LaunchState` and their Kubernetes and Docker
  implementations; the failure-reason code (`_pod_failure_reason`, the container state mapping)
  moves into `diagnose()`.
- `runs.dispatched()`, replaced by `runs.overdue()`.
- The reaper's launcher decision tree and its `created_at` timeout.

---

## 9. Testing

Test files mirror the package layout.

- Store: `claim_next` and `start` stamp `heartbeat_at`; `start` refuses a non-`dispatched` run;
  `heartbeat` returns `False` once terminal; `overdue` for each rule, `NULL` heartbeats ignored,
  timeout from job config, instance default, deleted job and `run_timeout = None`; `reap` skips a
  run whose heartbeat moved and a terminal run; `cancel` from every open status (quota released,
  hooks stamped, backfill advanced) and on a terminal run; `_finish` closes open executions and a
  late pod event deduplicates; backfill cancel of in-flight runs.
- Reaper: diagnosis appended, a raising `diagnose` tolerated, a skipped `reap` not counted.
- Executor: heartbeat exits on `False` and after `heartbeat_timeout/2` of errors (injected exit).
- Trigger (Postgres, `integration`): a heartbeat-only update does not notify; a status change does.
- API and toolkit: cancel route (role, 409) and `cancel_run`.
- Launchers: `diagnose` for Kubernetes and Docker against mocked clients.
- Live, on a dev instance: cancel a running demo run; kill a run container under the Docker
  launcher and observe it failed within about two minutes with its diagnosis.

---

## 10. Delivery

1. `refactor!: heartbeat-driven run liveness and timeouts`: migration, store verbs, `_finish`,
   heartbeat, sweep, `Job.timeout`, settings, `diagnose`, chart. Carries this spec.
2. `feat: cancel runs`: `runs.cancel`, API route, toolkit/MCP tool, UI, backfill cancel of
   in-flight runs.

Follow-ups (Jira ITLPR):

- `cloud-sql-proxy`: two replicas and a PodDisruptionBudget; today a single replica sits on every
  run's DB path.
- Default HTTP timeouts in `RESTClient`; 16 of 20 connector clients have none.
- Per-asset `KubernetesRunner`: delete child Jobs when the run is cancelled.
