# Store internals: one way to read, lock, save and derive

Review pass over interloper-db after the simplification programme (#423 to #429). The public surface had settled; this pass makes the private one read the same way in every facet and moves logic that belongs to the rows onto the rows.

## Problem

Each store facet had grown its own private idiom:

- Private helpers took a `session` parameter although `session_scope` already joins the ambient open session, so every write threaded a session it never needed to.
- Reads and writes fetched rows through different private helpers per facet (`_load_component`, `_lock_run`, `_identity`, `_latest_attempts`), some with eager loads that a `FOR UPDATE` select rejects on Postgres and that go stale once the write reshapes the row.
- Logic that only reads a row lived in the store: cancelling a run, superseding a retry head, sanitising an event payload, deriving a component's identity or retry policy.
- Naive SQLite datetimes were patched with `assume_utc` at every read instead of at the column.
- The same anti-join ("is this the latest attempt of its stack?") was written three times.
- Named catalog and run questions sat in `insights` because that is where they were first asked (`latest_by_target`, `asset_partitionings`, `activity`), not where their entity lives.
- Two methods answered the job-granularity question (singular and plural) with different drift rules.

## Decisions

1. **No session parameters.** A private helper runs inside `session_scope()`, which joins the transaction the public method opened. The few helpers that recurse or iterate within one scope take the session explicitly and carry an `_in` suffix (`_advance_in`, `_sync_children_in`).
2. **One fetch idiom per facet.** Public `get(id, *, org_id=None, ...)` reads a row with its load options; `org_id=None` means the caller authorises after the read (the API's `load_authorized` fetches first and checks `entity.org_id`; the scheduler reads across organisations). Private `_lock(id)` selects the bare row `FOR UPDATE` for a write (runs add `populate_existing`), with no eager options. A write that returns the row returns `self.get(...)` so the caller sees the reshaped unit.
3. **`session.save(session, row, *loads)`** adds, commits, refreshes and touches the named relationships, replacing the add/commit/refresh triplet in every `create`.
4. **The row owns what only reads the row.** `Run.cancel()`, `Run.supersede()`, `Event.from_event(event, org_id, run_id)` with the text and payload sanitisers, `Component.retry_policy`, `Component.identity`. `Component.qualified_key` reads the loaded parent and only lazy-loads when the row has a parent to load, so detached roots never touch the session.
5. **Naive datetimes are a test concern, handled in the tests.** Postgres hands `TIMESTAMPTZ` values back aware; only SQLite, which the suites run on, returns them naive. The repo-root `conftest.py` teaches the SQLite dialect to read datetimes back as aware UTC, once per process, so `assume_utc` leaves the store and no production type exists for a test dialect.
6. **One `latest_attempt()` expression** in `store/runs.py`, used by the run filters and the backfill counts.
7. **Questions live with their entity.** `runs.latest_by_target`, `runs.last_successes`, `components.asset_partitionings`, `components.target_assets`. The organisation feed becomes `insights.feed(org_id, PageQuery) -> Page[ActivityEntry]`, with `ActivityEntry` in `store/insights/feed.py`.
8. **One granularity question.** `components.job_partition_granularities(job_ids) -> dict[UUID, set[TimeGranularity]]`. The cron raises `ConfigError` when a job's assets disagree; insights pick the single granularity or none.
9. **`TimePartition.from_key` raises `ConfigError`** (a `ValueError`), so the store no longer wraps it in `parse_partition`.
10. **Events save through `dialect_insert`** on every dialect, so tests stop monkeypatching a SQLite path.
11. **Backfills expose `advance(backfill_id, *, failed)`** and the relations facet exposes its sibling API (`sync`, `check_bound`, `bind_siblings`, `detaches`) since the component store calls them across the module boundary.
12. **Exports narrow.** `interloper_db.store` exports `transaction` from the session module and nothing else from it; `ActivityEntry` comes from `interloper_db.store.insights`.

## Breaking

- `RunStore.parse_partition`, `InvitationStore.get`, `OrganisationStore.activity`, `ComponentStore.job_partition_granularity` (singular) and `InsightStore.latest_by_target` / `asset_partitionings` are removed in favour of the methods above.
- `interloper_db.store` no longer re-exports `commit`, `session_scope` or `ActivityEntry`.
- `TimePartition.from_key` raises `ConfigError` instead of `ValueError` (still a `ValueError` subclass).

## Size

Source net −231 lines (interloper-db −237, api −6, toolkit +6, scheduler +6); tests +49.
