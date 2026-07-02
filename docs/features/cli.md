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

## Run manifests

A **run manifest** is a YAML file that declares a DAG to materialize — sources, their config and
destinations, an optional asset selection, the runner, and the partition — without writing any
Python. It is loaded by `RunManifest`, compiled into a `RunPlan` (a ready `DAG` + partition +
runner), and executed.

```sh
interloper run -f manifest.yaml --dry-run   # validate + print the plan
interloper run -f manifest.yaml             # materialize
```

```yaml
name: demo-manifest

runner:
  type: serial          # serial | async | multi_process | docker | kubernetes
  # config: { max_workers: 4 }

# Reusable components referenced by alias with {ref: <alias>}.
resources:
  gcp:
    type: google_cloud_connection
    config:
      service_account_key: ${GCP_KEY}   # ${VAR} is interpolated from the environment

destinations:
  files:
    type: interloper.destination.file.FileDestination
    config:
      base_path: /tmp/interloper-demo

assets:
  - source: interloper_assets.demo.source.DemoSource
    config:
      hello: manifest
    destinations: [{ref: files}]    # one or more; writes fan out to each
    select: [a, b]                  # subset of the source's assets; omit to run all

partition:
  date: 2026-01-01
  # or a window:
  # start: 2026-01-01
  # end: 2026-01-31
```

A component `type` is either a **catalog key** (resolved against the `catalog` in
`interloper.yaml`) or a fully qualified import path. The `runner` and `partition` blocks are
overridden by the CLI `--date` / `--start-date` / `--end-date` flags when present.

In Python:

```py
import interloper as il

plan = il.RunManifest.from_yaml_file("manifest.yaml").compile()
plan.dag.materialize(partition_or_window=plan.partition)
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
