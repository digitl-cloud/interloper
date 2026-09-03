# CLI flags

```
interloper <command> [options]
```

Without a command, the help is printed. A `.env` file in the working directory is loaded when
`python-dotenv` is installed. Telemetry is initialized from settings for every command.

## `interloper run`

Execute a DAG with the configured runner, without persistence.

| Argument | Meaning |
|----------|---------|
| `target ...` | Import paths of source or asset classes (`--format paths`), or one JSON `DAGSpec` (`--format inline`). |
| `-f, --file PATH` | A runnable component spec (YAML). Mutually exclusive with positional targets. |
| `--format {paths,inline}` | Input format. Default `paths`. |
| `--date KEY` | One partition. The key's shape carries the granularity: `2026-01-15`, `2026-01`, `2026`, `2026-01-15T13`. |
| `--start-date KEY`, `--end-date KEY` | A window. Both required together, same granularity, exclusive with `--date`. |
| `--dry-run` | Validate and print the plan without executing. |
| `--events {pretty,json}` | Render events through the logger (default) or stream raw JSON lines to stdout. |
| `-q, --quiet` | Warnings and errors only. Takes precedence over `-v`. Does not affect `--events json`. |
| `-v, --verbose` | Debug verbosity, including `data()` and destination I/O events. |
| `--run-id ID` | Run identifier forwarded as metadata. |

Exit status is non-zero on invalid input, import failure, or a run that did not complete.

## `interloper db`

Requires `interloper-db`.

| Subcommand | Meaning |
|------------|---------|
| `init` | Ensure the database exists, create tables, migrate to head. Idempotent. |
| `reset [-y]` | Drop and recreate the database. `-y` skips the confirmation. |
| `upgrade [REVISION]` | Run migrations to `REVISION` (default `head`). |
| `downgrade [REVISION]` | Downgrade migrations (default `-1`). |

## `interloper app`

Requires `interloper-db`; `--api` needs `interloper-api`; `--cron`, `--worker` and `--reaper`
need `interloper-scheduler`.

| Flag | Meaning |
|------|---------|
| `--host HOST` | Server bind host (default from settings, `0.0.0.0`). |
| `--port PORT` | Server bind port (default from settings, `3000`). |
| `--api / --no-api` | Run the API server. |
| `--cron / --no-cron` | Run the cron controller (with hook and renewal controllers). |
| `--worker / --no-worker` | Run the queue worker. |
| `--reaper / --no-reaper` | Run the reaper. |
| `--dev` | Run the Nuxt dev server instead of serving built assets. |
| `--no-create-tables` | Skip the table bootstrap on startup. |

Flags left unset fall back to the `server`, `cron`, `worker` and `reaper` settings. At least one
service must be enabled.

## `interloper launch RUN_ID`

Requires `interloper-db` and `interloper-scheduler`. Executes one persisted run; on any failure
before the executor takes over, the run is marked failed.

## `interloper agent`

Requires `interloper-agent`.

| Flag | Default |
|------|---------|
| `--host` | `127.0.0.1` |
| `--port` | `8000` |
