# Insights: one owner for derived reads

Step 3 of the interloper-db / interloper-api simplification. Step 2a (store facets, pages, HTTP conventions) shipped in #425.

## Problem

The overview route (`routes/overview.py`, 1006 lines) held domain logic: which jobs are failing or overdue, what needs attention, the coverage calendar, the next firing's window. The toolkit re-derived overlapping answers with different definitions:

- `freshness_check` called a job stale after 24 hours without a success; the overview called it failing when its latest attempt failed.
- `partition_coverage` counted a partition covered only when a whole run of the job succeeded; `asset_coverage` and the calendar counted any successful execution of the asset.
- `run_history_summary` and `run_stats` both counted outcomes, one per attempt, one per stack.
- `get_job_health` computed a success rate over the last 20 runs, a third notion of health.
- Error groups merged on a classifier in the toolkit, and on raw text in the overview.
- The scheduler and the API each resolved a job's timezone and lookback window.

## Decisions

1. **`store.insights` in interloper-db** returns frozen dataclass facts. Wording a fact for a reader stays with its consumer (the overview's titles, the toolkit's paging and missing ranges). The aggregate queries behind the facts live in the facet, so the performance pass has one place to land.
2. **One definition each.**
   - A run's outcome is its stack's, by the latest attempt.
   - A job is *failing* when it is enabled and its latest attempt failed; *overdue* when its stored next slot passed more than 15 minutes ago.
   - An asset is failing when its latest execution failed, and its source with it; a hook when it recorded an error within 30 days.
   - Coverage counts executions from runs of any target.
   - Error groups merge on the classifier's fingerprint (`ErrorCause.from_text`).
3. **One job window.** `CronJob.zone(config)` and `CronJob.window(config, fires_at=, granularity=)` in core are what the scheduler fires and what the overview shows as upcoming.
4. **Tools consolidated.** `job_health` (new), `run_stats`, `asset_coverage` and `error_breakdown` read the facet. `partition_coverage`, `run_history_summary`, `get_job_health` and `freshness_check` are deleted.
5. **Facts are served directly where shapes match.** The overview returns `FinishedRuns`, `InFlight`, `BackfillProgress` and `KindInventory` as they are; `run_stats` returns `JobOutcome` rows. Response models exist only where a consumer adds wording or projection.

## Facet surface

| Method | Returns |
|---|---|
| `health(org_id, *, now)` | `OrgHealth`: each job's `JobHealth`, the failing set, `Attention` items, the inventory by kind |
| `activity(org_id, *, now)` | `Activity`: the last 24 hours by hour, what is in flight, backfill progress |
| `outcomes(org_id, *, since, until, job_id)` | `list[JobOutcome]`, most failed stacks first |
| `error_groups(org_id, *, since, until, job_id, backfill_id, run_id, group_by)` | `ErrorGroups`: groups, rows read, whether the 20k-row cap cut the read |
| `coverage_by_day(org_id, *, since, until, now)` | `list[CoverageGroup]` for the calendar |
| `coverage_by_key(org_id, job_id, *, start_key, end_key)` | `JobCoverage`: every target asset with its covered and failed keys |

Modules: `health.py`, `outcomes.py`, `failures.py`, `coverage.py` hold the facts and their constructors; `base.py` holds `InsightStore` and its queries (moved from `runs.latest_by_target`, `events.error_groups`, `executions.coverage_rows`, `components.asset_partitionings`).

## Breaking changes

- Toolkit / MCP: the four deleted tools; `run_stats` rows use `retried` and `duration_*_seconds`; `asset_coverage` lists every target asset, including those that never executed.
- Overview: attention error groups merge per job and classified cause, titled by the cause summary.
- `interloper_db.store` no longer exports `ErrorGroup`, `CoverageRow` or `PartitionExecution`; `interloper_api.utils.job_zone` is gone.

## Size

Source net +344 lines (api −740, toolkit −426, scheduler −49, core +79, db +1483), tests +8. The estimate was a reduction: it counted the duplicate derivations removed but not the typed fact layer that replaces them.
