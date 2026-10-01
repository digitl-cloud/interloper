# Overview page

The app's landing page. Today `/` redirects to `/graph`; it becomes an overview that answers
"is my data OK, and what needs me?" at a glance.

Source of truth for layout and styling: `UI - Overview.dc.html` in the Claude Design project
(`fa3b2c24-1141-4650-9ea0-9a3aea9609c6`). This spec covers what the design needs from the data,
how each section is defined, and what is deliberately left out.

## Scope

Built, in page order:

1. Health strip (four tiles)
2. Needs attention (list, or an "All clear" state)
3. Timeline (past runs and scheduled runs around now)
4. Partition coverage (calendar heatmap with a day detail panel)
5. Coming up / Just happened (two lists)
6. Components (per-kind state inventory)

Rule for what is in: anything derivable from data the platform already stores ships, even when it
needs a new query or endpoint. Anything needing new stored data, a new write capability or a new
dependency is an open point (end of this document).

## Definitions

These are the page's vocabulary; every section uses them the same way.

- **Stack**: a run and its retries (`root_run_id`), read by its latest attempt.
- **Failing job**: an enabled job whose most recently created attempt targeting it ended
  `failed`, whatever stack that attempt belongs to. Manual and scheduled backfills are indistinguishable in the
  data, so no distinction is made.
- **Overdue job**: an enabled job whose `state.next_run_at` is more than 15 minutes in the past.
- **Drift**: a component whose `ComponentStatus` is not `ok` (`disabled`, `missing`,
  `unreadable`).
- **Connection needing re-authorisation**: a connection whose `state.last_renewal_error` is set.
- **Backfill in progress**: a backfill with status `queued` or `running`. Cron firings of
  partitioned jobs are backfills and count here.

## Sections

### 1. Health strip

Four tiles, each a link.

| Tile | Headline | Detail | Visual | Links to |
|---|---|---|---|---|
| Runs, last 24h | attempts that completed in the last 24h (whether or not they started) | succeeded, failed | 24 hourly stacked bars (success/failed) by completion hour | `/executions/runs` |
| Running now | running attempts | queued attempts | running vs queued bar; longest running duration | `/executions/runs` |
| Backfills in progress | count | partitions done of total | combined progress bar and % | `/executions/backfills` |
| Jobs failing | failing jobs | of N enabled | failing vs healthy bar | jobs page |

"Done" partitions are a backfill's runs in a terminal status (`success`, `failed`, `canceled`),
from the existing `run_counts`.

### 2. Needs attention

One bordered list, sorted by severity then recency. Each row: severity icon, title, a meta line
(kind, target, when), an "open" link, and, for editors, a fix button.

| Item | Severity | Title | Target | When | Open | Fix (editor) |
|---|---|---|---|---|---|---|
| Error group (last 24h, grouped by job and the error's first line) | error | "{runs} runs failed: {error first line}" | job name | last seen | sample run | none (open point) |
| Connection needing re-authorisation | error | "{name} connection needs re-authorisation" | error text, first line | last renewed, when known | connection | Reconnect |
| Stack still failing after retries (latest attempt failed, attempt > 1, finished in the last 24h) | error | "Still failing after {n} attempts" | job · partition | finished at | run | none |
| Drift | warning | per status: "missing from catalog", "disabled in this deployment", "config unreadable" | component name | none (open point) | collection | none |
| Overdue job | warning | "Scheduled run is {duration} overdue" | job name | due time | job | none |

"Reconnect" opens the connection's edit form (which carries the OAuth sign-in) through a new
`?edit=<id>` query on the connections page, mirroring the existing `?new=` deep link.

Empty list: the "All clear" card, with the time the overview was last fetched.

### 3. Timeline

The existing `ChartExecutionTimeline` and `useRunTimelineRows`, embedded with the existing
`TIMELINE_SPANS` (1h, 6h, 12h, 24h, 7d, 30d). Additions:

- The window is two thirds past, one third future, around now.
- The future region is hatched; a "Now" line and label mark the present.
- Each enabled job's next firing (`state.next_run_at`), when it falls inside the future region,
  renders as a dashed `scheduled` bar. Its length is the job's latest completed run duration in
  the window, floored to the chart's minimum width. Only the next firing is known (open point).
- Header shows the display timezone and the window's range; legend lists Success, Failed,
  Running, Queued, Scheduled.

Rows stay what `useRunTimelineRows` produces today: jobs, ad-hoc asset runs, connection renewals.

### 4. Partition coverage

A calendar heatmap (ECharts `calendar` + `heatmap`), keyed by **partition date**, with a job
filter ("All jobs" or one job) and a 3, 6 or 12 month window (default 6). Cell size shrinks as
the window grows, following the design (22, 15, 11 px).

Per job, per day:

- **Assets counted**: those the job executed at least once in the window.
- **Days counted**: from the job's first attempted partition in the window to the last day of the
  last period that closed before today (yesterday for a daily or hourly job, the end of last month
  for a monthly one), or to its last attempted partition when later (the open period counts once
  attempted). A disabled job stops at its last attempted partition.
- **Granularity**: an hourly partition counts toward its day (24 per asset per day; today, the
  hours elapsed since midnight UTC, at least one, or the hours attempted when more); a monthly or
  yearly partition counts toward every day it spans, up to today.
- **expected** = asset-partitions for the day; **covered** = those with a successful execution in
  any run; **failed** = attempted, never succeeded; **missing** = the rest.

Cell colour for a day (summed over the filtered jobs), matching the design:

- nothing expected: neutral with an inset border ("Not expected")
- any failed: red, stronger with the failed share
- covered = expected: green
- covered at least 60%: amber; below: light amber

Summary line: "{covered %} of {expected} partitions · {n} days with gaps · {n} with failures".

Clicking a day opens the detail panel under the calendar: per job a covered/failed bar and a
count, then one action: "Complete", "Open run" (a sample failed run of that job and day), or, for
editors when there are gaps without failures, "Backfill", which opens `RunModal` preset to that
job and day.

### 5. Coming up / Just happened

- **Coming up**: enabled jobs ordered by their next firing (`state.next_run_at`, first 4), each
  with its time (display timezone), job, the partition range it will cover, and a relative time.
  The partition range comes from `TimePartitionWindow.lookback` with the job's `lookback`,
  `offset` and target granularity, the same resolution the scheduler uses. Unpartitioned jobs
  show no partition. One slot per job: firings beyond the next are an open point.
- **Just happened**: the latest 5 stacks, with status badge, partition and finish time.

### 6. Components

A table of kinds (Sources, Assets, Destinations, Connections, Jobs, Hooks): icon and label, count,
a proportional state bar, and an issues summary ("1 failing · 1 needs attention", or "all
healthy"). Header summary: "{total} in the collection · {n} need attention". Each row links to
its kind's page.

Each component gets exactly one state, first match wins:

1. **disabled**: its config has `enabled: false`
2. **failing**: asset whose latest execution failed; source with a failing asset; failing job;
   hook whose latest hook event is `hook_failed`
3. **needs attention**: drift; connection needing re-authorisation
4. **healthy**: everything else

Destinations have no health signal of their own, so they are only ever disabled, needs attention
or healthy.

## Architecture

### Backend

The cut is the one the codebase already makes: `interloper-db` owns what the data says,
`interloper-api` owns what it means for the reader. No new package, no new dependency, no new
edge in the dependency graph.

```
interloper-core        framework, partitioning (TimePartitionWindow)
interloper-db          store: rows and aggregate reads      -> core
interloper-api         HTTP: composes rows into the page    -> core, db
interloper-toolkit     agent/MCP analysis                    -> core, db   (untouched)
interloper-scheduler   cron                                  -> core, db   (untouched)
```

**Store** (`interloper-db`), aggregate reads next to their siblings (`error_groups`,
`partition_coverage`, `latest_executions`), each returning typed rows that know nothing about
the page:

- `runs.latest_by_target(org_id, component_kind=)`: per target, its most recently created
  attempt (ties on creation broken by the later partition key, then the id).
- `runs.list_all(org_id, completed_after=, completed_before=, sort="-completed_at")`: attempts or
  stacks by the instant they completed, which a run that failed before starting still has. The
  Runs tile reads every attempt completed in the last 24h (bucketed by completion hour in the
  route), the attention list the failed stacks completed in that window, and "Just happened" the
  latest completed stacks, most recently completed first.
- `events.coverage_rows(org_id, since, until)`: the org-wide sibling of `partition_coverage`:
  one row per job, partition, asset with whether it ever succeeded, whether any execution
  failed, plus a failed run id. An attempted asset-partition that neither succeeded nor failed
  (still in flight, or canceled) stays missing rather than failed.
- `events.latest_by_component(org_id, event_types=, since=)`: the latest `hook_fired` /
  `hook_failed` event per hook, bounded to the last 30 days.
- `components.job_partition_granularities(job_ids)`: each job's target granularity in one read.

**API** (`interloper-api`), new `routes/overview.py`, viewer role:

- `GET /overview`
- `GET /overview/coverage?since=&until=`

The route module owns the page's semantics: the definitions above, the component state
precedence, the coverage day rules and the attention ordering, as pure functions over store rows,
the way `routes/components.py` owns `ComponentResponse.from_row`. Error groups come from
`events.error_groups` merged by job and first line in the route. The upcoming partition of a job
comes from `TimePartitionWindow.lookback` and `components.job_partition_granularities`.

The toolkit's `analytics.py` overlaps in spirit, but its outputs are LLM-shaped. A shared
read-model package is only worth extracting once a second consumer needs these definitions
(open point).

### Frontend

- `pages/index.vue` renders the overview (title "Overview"), replacing the redirect. An "Overview"
  destination heads the sidebar in `composables/navigation.ts`.
- `stores/overview.ts` fetches both endpoints independently (the calendar is the slow one) and
  refetches on `runs` and `backfills` realtime notifications, throttled to one refresh per 15 s;
  the calendar refetches only when a run reaches a terminal status.
- Components under `components/overview/`: health tiles, attention list, coverage calendar, day
  detail, coming up, just happened, components inventory.
- `ChartExecutionTimeline` gains a `scheduled` bar kind and a hatched future region.
- Role gating reads `userStore.user.role` (`viewer` hides fix buttons and backfill actions). This
  is the first role-aware UI in the app.
- Colours come from `CHART_STATUS_COLORS` (canvas) and Nuxt UI semantic classes (DOM), so dark
  mode works; the design is light-only. Design icons (Phosphor) map to lucide.

## Testing

- Store queries: `interloper-db` tests folded into `tests/store/test_runs.py` and
  `tests/store/test_events.py`.
- API: `tests/routes/test_overview.py` covering auth, org scoping, response shape, and every
  definition above (failing, overdue, drift, renewal error, component state precedence, coverage
  day rules including hourly and monthly granularity, upcoming partition ranges).
- Frontend: lint, typecheck, and a headless pass on a seeded dev instance in light and dark mode.

## Open points

- **Retry all** on an error group: no bulk retry endpoint exists; the row links to its sample run.
- **Firings beyond the next one**: only `next_run_at` is stored; showing a job's whole week on the
  timeline needs cron expansion (`croniter`), which only the scheduler depends on.
- **Error grouping by cause**: the toolkit's classifier is not shared with the API; groups merge
  on the error's first line.
- **Shared read model**: the overview definitions live in the API route; the agent and MCP would
  need a common package to reuse them.
- **Drift "since"**: when a component entered drift is not recorded.
- **Destination health**: no signal exists beyond drift and enabled.
- **Renewal failure time**: connection state records the last successful renewal, not when the
  failing attempt happened.
- **New organisation checklist**: deferred.
