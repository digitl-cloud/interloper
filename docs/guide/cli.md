# CLI

The `interloper` command runs DAGs directly and operates a deployed instance. This page covers
`run`, the framework-level command; the others exist when platform packages are installed and
are listed at the end.

## Running a DAG

Run one or more components by import path:

```sh
interloper run my_package.sources.Shop
interloper run my_package.sources.Shop my_package.sources.Finance
```

Each path resolves to a source or asset class, which is instantiated with its defaults and
handed to `DAG(*items)`. The module must be importable from the current environment: an
installed package, or a directory added with `PYTHONPATH`. Because defaults rarely include
destinations, a DAG with dependencies usually needs a spec file instead; a `paths` run is most
useful with `--dry-run` or for sources that declare destinations in code.

### From a spec file

```sh
interloper run -f shop.yaml
```

The file is a [spec](specs.md) for any runnable component: a source, an asset, or a job with
its targets and workload-level defaults. `${VAR}` placeholders are interpolated from the
environment. `-f` cannot be combined with positional targets.

### From an inline DAG spec

```sh
interloper run --format inline '{"items": [...]}'
```

Takes a serialized `DAGSpec` as JSON. Container runners use this mode to hand a mini-DAG to a
child process; it is rarely typed by hand.

## Partitions

Date flags take partition **keys**, whose shape carries the granularity:

```sh
interloper run shop.Shop --date 2026-01-15                          # one daily partition
interloper run shop.Shop --date 2026-01                             # one monthly partition
interloper run shop.Shop --date 2026-01-15T13                       # one hourly partition
interloper run shop.Shop --start-date 2026-01-01 --end-date 2026-01-07   # a window
```

Both bounds of a window must share one granularity. A window is a single run covering the
range, so every partitioned asset must allow windows; for one run per partition, loop in a
shell or in Python (see [Backfilling](backfilling.md)).

## Output

Run and operation events flow through the logging stack, sharing one format and stream with
ordinary log lines on stderr:

```
19:39:52.368 INFO    Running DAG with 3 materializable operation(s) (3 total) using AsyncRunner
19:39:52.370 INFO    RUN_STARTED      -  Run started (3 operations)
19:39:52.370 INFO    OPERATION_STARTED users  Operation 'users' started
19:39:52.371 INFO    OPERATION_STARTED orders  Operation 'orders' started
19:39:52.692 INFO    OPERATION_COMPLETED users  Operation 'users' completed
19:39:52.692 INFO    OPERATION_COMPLETED orders  Operation 'orders' completed
19:39:52.692 INFO    OPERATION_STARTED order_count  Operation 'order_count' started
19:39:52.694 ERROR   OPERATION_FAILED order_count  Operation 'order_count' failed: AssetError: No destination found for upstream asset 'orders'
19:39:52.694 ERROR   RUN_FAILED       -  Run failed (1 operation(s) failed)
19:39:52.694 INFO    Run completed: FAILED
```

The failure above is the one a `paths` run without destinations produces; a spec file with
destinations fixes it.

| Flag | Effect |
|------|--------|
| `-v` | Debug verbosity: also show `data()` and destination I/O events. |
| `-q` | Warnings and errors only. |
| `--events json` | Stream raw event JSON lines on stdout instead, unaffected by the level. |
| `--dry-run` | Validate and print the plan (runner, partition, operations by generation) without executing. |
| `--run-id ID` | Forward a run id in the metadata. |

The process exits non-zero when the run does not complete.

## The runner

The runner comes from `AppSettings`: the `runner` block of `interloper.yaml` or
`INTERLOPER_RUNNER_TYPE` / `INTERLOPER_RUNNER_CONFIG`. Built-in types are `async` (default),
`serial` and `multi_process`; companion packages add `docker` and `kubernetes`.

```yaml
# interloper.yaml
runner:
  type: async
  config:
    max_workers: 8
```

## Environment

A `.env` file in the working directory is loaded when `python-dotenv` is installed. Telemetry
is initialized from the `otel` settings for every command. In a container with
`INTERLOPER_EVENTS_TO_STDERR=true`, events are forwarded as `@EVENT:` lines instead of being
printed; see [Events](events.md#events-across-processes).

## Other commands

| Command | Requires | Purpose |
|---------|----------|---------|
| `interloper db init\|reset\|upgrade\|downgrade` | `interloper-db` | Database provisioning and migrations. |
| `interloper app` | `interloper-db`, plus `interloper-api` and `interloper-scheduler` for the corresponding services | Run the API, cron controller, queue worker and reaper. |
| `interloper launch <run_id>` | `interloper-db`, `interloper-scheduler` | Execute one persisted run. |
| `interloper agent` | `interloper-agent` | Start the agent development UI. |

A command whose packages are missing exits with a message naming them. Every flag is listed in
[CLI flags](../reference/cli.md).
