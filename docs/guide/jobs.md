# Jobs

A job is a named, schedulable workload: it declares **what** to materialize and, for a cron job,
**when**. The core carries the declaration and compiles it to the same DAG every other entry
point runs. Acting on the schedule is the job of the platform scheduler, outside this site.

## Job

```py
import interloper as il

job = il.Job(
    targets=[Shop(account_id="act_1"), Finance()],
    destinations=[warehouse],
    tags=["daily"],
)
il.DAG(job).materialize(partition)
```

| Field | Meaning |
|-------|---------|
| `targets` | Sources and assets to materialize. Their operations are flattened into the DAG. |
| `destinations` | Defaults for any target that declares none. |
| `enabled` | A disabled job is kept but not scheduled. |
| `tags` | Free-form labels. |

Cascading works exactly like a source's: a job-level destination reaches every target without
one. A job subclass that declares a further relation (a connection, say) cascades it the same
way, into every target and destination declaring the same relation name and leaving it unbound.

`JobState` (`next_run_at`, `last_run_at`) is the job's machine-owned state, written by the
scheduler. Editing a job's config clears `next_run_at`, so the next tick re-derives the schedule
from the new spec rather than firing once more at the slot the old one produced.

## CronJob

`CronJob` adds the trigger:

```py
job = il.CronJob(
    cron="0 6 * * *",
    timezone="Europe/Berlin",
    lookback=3,
    offset=1,
    targets=[Shop(account_id="act_1")],
)
```

| Field | Default | Meaning |
|-------|---------|---------|
| `cron` | required | When the job runs, on the wall clock of `timezone`. |
| `timezone` | `"UTC"` | IANA zone the schedule is evaluated in. Unknown names are rejected. |
| `lookback` | `1` | How many partitions each run covers. |
| `offset` | `1` | How many partitions back from the current one the window ends. |

For daily targets the defaults mean "yesterday, in the job's timezone". `offset=3, lookback=3`
covers the three days ending three days ago, for a vendor whose numbers settle late. The window
itself is computed with `TimePartitionWindow.lookback`; hourly targets always use UTC windows
because hour ids are UTC labels. Whether a job is partitioned is derived from its targets, never
stored.

## Jobs as specs

A job is the natural unit for a declarative run. Its spec lists targets with their configuration
and the workload-level defaults. A connection a target needs is not a job field, so it is nested
under that target's own relation name, in the target's own `init`:

```yaml
path: interloper.job.base.Job
init:
  destinations:
    - key: bigquery_destination
      init:
        project: dwh
        location: EU
        connection:
          key: google_cloud_connection
          init:
            service_account_key: ${GCP_KEY}
  targets:
    - key: facebook_ads
      init:
        account_id: act_1
        connection:
          key: facebook_ads_connection
          init:
            access_token: ${FB_TOKEN}
            app_id: "1"
            app_secret: ${FB_SECRET}
        select: [campaigns, ads_stats]
```

`interloper run -f job.yaml --date 2026-01-15` reconstructs it and runs its DAG. See
[Specs and serialization](specs.md) and [CLI](cli.md). A scheduled job is the same document
with `key: cron_job` and the trigger fields.

## Relations

A job's relation vocabulary is `targets` (sources and assets) and `destinations`. A target is an
orchestration pointer, not an input: deleting a target shrinks the job rather than blocking the
deletion. Relation semantics are described in the
[component model](../extending/components.md#relations).
