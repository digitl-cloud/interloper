# CLI & Manifests

Interloper includes a command-line interface for running DAGs and operating an instance.

```sh
interloper <command> [options]
```

| Command | Purpose |
|---------|---------|
| `run` | Materialize a DAG, from import paths or a manifest |
| `db` | Database operations (`init`, `reset`, `upgrade`, `downgrade`) — needs `interloper-db` |
| `app` | Start application services (API, cron, worker, reaper) — needs `interloper-db` |
| `launch <run_id>` | Execute a single persisted run by id |
| `agent` | Launch the agent development UI — needs `interloper-agent` |

This page focuses on `run`; the others belong to a deployed instance.

## Running a DAG

Run one or more components by import path:

```sh
interloper run interloper_assets.demo.source.DemoSource
```

### Partitions

```sh
interloper run <target> --date 2025-01-15                       # single partition
interloper run <target> --start-date 2025-01-01 --end-date 2025-01-07   # window (backfill)
```

### Other options

| Option | Description |
|--------|-------------|
| `-f, --file PATH` | Run a declarative manifest (see below) |
| `--dry-run` | Validate and print the plan without executing |
| `--date ISO` | Single partition date |
| `--start-date ISO` / `--end-date ISO` | Partition window |
| `--events [pretty\|json]` | Event output format (default `pretty`) |
| `--run-id ID` | Optional run identifier forwarded to the runner |
| `-q, --quiet` / `-v, --verbose` | Decrease / increase log verbosity |

## Declarative workloads

`interloper run -f` executes a **component spec** — the same `{path|key, init}` document that
`Component.to_spec()` emits — for any *runnable* component: the run is literally "reconstruct the
component and run its DAG". A component is referenced by exactly one of `key` (a catalog key,
resolved against the `catalog` in `interloper.yaml`) or `path` (a fully qualified import path).
`${VAR}` placeholders in any string value are interpolated from the environment at load time.

A **Job spec** is the composite form: targets plus workload-level defaults, cascading exactly like
a source cascades to its assets — job `destinations` apply to any target that declares none, and
job `resources` fill empty resource slots of targets and destinations by name, then by type.

```sh
interloper run -f job.yaml --dry-run              # validate + print the plan
interloper run -f job.yaml --date 2026-01-01      # materialize
```

```yaml
path: interloper.job.base.Job    # ad-hoc workload; scheduled jobs are `key: cron_job`
init:
  resources:
    gcp:
      key: google_cloud_connection
      init:
        service_account_key: ${GCP_KEY}   # interpolated from the environment

  destinations:
    - key: bigquery_destination           # its connection slot fills from the job resources

  targets:
    - key: facebook_ads
      init:
        dataset: raw_facebook
        select: [campaigns, ads]          # subset of the source's assets; omit to run all
    - path: my_package.assets.OneOffAsset # sources and assets are both just targets
```

A plain source or asset spec runs directly too:

```yaml
key: demo_source
init: { hello: spec }
```

The runner comes from `interloper.yaml` / `INTERLOPER_RUNNER_*` environment variables; the
partition from the CLI `--date` / `--start-date` / `--end-date` flags. In Python, the same
document is one call away:

```py
import interloper as il

dag = il.DAG.from_spec_file("job.yaml")
dag.materialize(partition_or_window=il.TimePartition(...))
```

## interloper.yaml

A repository-root `interloper.yaml` configures a deployed instance: the Postgres connection, the
default runner and launcher, the server/cron/worker/reaper services, and the **catalog** — the
list of component import paths to enable. Every field also has an `INTERLOPER_*` environment
variable equivalent.

```yaml
runner:
  type: async

catalog:
  - interloper_assets.facebook_ads.source.FacebookAds
  - interloper_assets.google_ads.source.GoogleAds
  # ...
```

The catalog defined here is what `il.Catalog.from_settings()` loads and what manifest `type`
keys resolve against. See [Catalog](catalog.md).
