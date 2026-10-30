# CLI

The `interloper` command runs DAGs directly and operates a deployed instance. This page covers
`run`, the framework-level command; the others exist when platform packages are installed and
are listed at the end. Without a command, the help is printed. A `.env` file in the working directory
is loaded when `python-dotenv` is installed, and telemetry is initialized from settings for every command.

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
19:39:52.368 INFO    Running DAG with 3 enabled operation(s) (3 total) using AsyncRunner
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
| `--operation-id ID` | Execute only that operation of the DAG, once, its upstreams read rather than executed, leaving what it came to for the host run that started the process. |

The process exits non-zero when the run does not complete.

## The runner

The runner comes from `AppSettings`: the `runner` block of `interloper.yaml` or
`INTERLOPER_RUNNER_TYPE` / `INTERLOPER_RUNNER_CONFIG`. Built-in types are `async` (default),
`serial`; companion packages add `docker` and `kubernetes`.

```yaml
# interloper.yaml
runner:
  type: async
  config:
    max_workers: 8
```

## Environment

A `.env` file in the working directory is loaded when `python-dotenv` is installed. Telemetry
is initialized from the `otel` settings for every command.

## Platform runs

`interloper launch <run-id>` is what a launcher starts to execute a platform run, not a command
authors type. With the run's token in `INTERLOPER_RUN_TOKEN`, it loads the run from the API at
`server.internal_url` (its DAG, its partition and the metadata every event carries) and reports
its events there instead of printing them; see [Events](events.md#persistence). Without a token,
it stops with what it needs. A child a container runner starts for one operation of that run runs
`interloper launch <run-id> --operation-id <id>`: it executes that operation once and reports only
what happens inside it. Outside the platform, a container runner's child is handed the
operation's DAG instead, `interloper run --format inline <dag> --operation-id <id>`.

## Other commands

Each platform package contributes its own commands, so a command exists exactly when its
package is installed:

| Command | Package | Purpose |
|---------|---------|---------|
| `interloper api` | `interloper-api` | Serve the HTTP API: the api role's command. |
| `interloper oauth create\|list\|delete` | `interloper-api` | Register, list and delete the OAuth clients an operator provides for apps that cannot register themselves; see [Connecting MCP clients](../ui/connecting-clients.md#registering-a-client-yourself). |
| `interloper scheduler` | `interloper-scheduler` | Run cron (with hooks and credential renewal), the queue worker and the reaper: the scheduler role's command. |
| `interloper db init\|reset\|upgrade\|downgrade` | `interloper-db` | Database provisioning and migrations. |
| `interloper app` | `interloper-app` | The API, the scheduler's services and the frontend in one process, for local development and single-process deployments (`--dev` runs the Nuxt dev server). |

Every flag is listed in [CLI](../reference/cli.md).
