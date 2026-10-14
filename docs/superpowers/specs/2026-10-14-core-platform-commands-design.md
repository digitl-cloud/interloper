# Core phase 6a: platform out of core

Date: 2026-10-14. Status: approved in conversation, implementation pending.

Phase 6 of the interloper-core consolidation programme is the periphery pass (`cli`, `settings`,
`errors`, `telemetry`, `rest`, `oauth`). It is split into three stacked sub-phases:

- 6a, this spec: platform code leaves core;
- 6b: telemetry and logging;
- 6c: connection I/O (`rest` and `oauth`).

Phases 1 to 5 reshaped the kernel and were rebased onto main 0.112.0 before this phase started.

**The problem.** `interloper-core` hosts the `app`, `db` and `launch` commands and the 532-line
`cli/runtime.py`. They import `interloper_api`, `interloper_db`, `interloper_scheduler` and
`interloper_app`, none of which core depends on, so they rely on lazy imports and a
`requires`/`requires_when` check that misses the default case. It also declares six errors that
only `interloper-db` raises and only db, api, toolkit and scheduler catch.

**The goal.** Each platform package owns its commands and errors, and core knows nothing about the
platform. The surveys also found several CLI bugs, fixed here.

**Not in scope.**
- Settings stay one typed document in core (`AppSettings`); nothing about them changes beyond the
  DSN fix in section 5. Package-declared settings will be designed once, together with the
  extension-package system, for platform packages and extensions alike.
- Console logging setup, the `INTERLOPER_EVENTS_TO_STDERR` check and the telemetry lifecycle move
  as they are and are 6b's.

## 1. Commands are owned by their packages

A command is contributed through one entry-point group, `interloper.cli`. Each entry names a
module-level `register(subparsers)` function, the shape every command already has (CLI command
wiring is a permitted module-level function). The extension-package design reuses the same group
later.

| Command | Package | Module |
|---|---|---|
| `run` | interloper-core | `interloper/cli/commands/run.py` |
| `db` (init, reset, upgrade, downgrade) | interloper-db | `interloper_db/cli.py` |
| `launch` | interloper-scheduler | `interloper_scheduler/cli.py` |
| `api` (new, section 2) | interloper-api | `interloper_api/cli.py` |
| `scheduler` (new, section 2) | interloper-scheduler | `interloper_scheduler/cli.py` |
| `app` | interloper-app | `interloper_app/cli.py` |

Core declares its own `run` entry in the same group, so the parser has one path: `build_parser()`
registers every installed entry, sorted by name, so `--help` and the generated CLI reference are
stable. A command exists exactly when its package is installed. Running `interloper app` on a
core-only install gets argparse's own `invalid choice: 'app' (choose from 'run', ...)`.

`main()` keeps the lifecycle every command shares and nothing command-specific:

1. load `.env`;
2. parse;
3. activate settings;
4. start telemetry;
5. call the handler;
6. shut down telemetry and clear settings.

The `app` special case (`apply_cli_overrides` before activation) leaves `main()`. A command whose
flags override settings (`api`, `scheduler`, `app`) re-activates the active settings with its
overrides at the start of its handler. The overrides never touch `otel`, so telemetry started by
`main()` is unaffected.

**Removed:**
- `requires`, `requires_any`, `requires_when`, `_has_package` and `_enforce_requires`;
- the dead `--help` fallback in `main()`;
- `runtime.py`, whose content moves to the role packages (section 2).

`dev/docs_macros.py`'s `cli_reference()` stops reading `requires`/`requires_when`; it renders the
parser `build_parser()` returns, now holding every installed command.

## 2. One command per role

The `api` and `scheduler` images each run `interloper app` with only their own half installed. The
api image has core, db and api; the scheduler image has core, db and scheduler; neither has
`interloper-app`. A role therefore gets its own command, owned by its package, and `interloper app`
composes them.

### 2.1 The supervisor (core)

Every serving command runs several blocking loops until a signal arrives. That lifecycle is one
small class in core, `interloper.utils.Supervisor`. It is type-agnostic: it knows nothing about
api or scheduler.

- It takes the services to run. A service is anything with a `name`, a blocking `start()` and a
  `stop()`, which the scheduler's controllers already have.
- It runs each service in its own daemon thread.
- It installs SIGINT, SIGTERM and SIGHUP handlers.
- On a signal, or when any service's thread ends unexpectedly, it stops every service, joins the
  threads with a grace period and logs `Shutdown complete.`

This is the lifecycle `Services.run`, `_start_threads`, `_install_signal_handlers`, `_on_signal`
and `_shutdown` implement today, given one home, with one deliberate change. Today a service whose
thread dies (the API failing to bind its port, a controller crashing) leaves the process up with
the other services running and the dead one silently gone, so a pod looks alive while serving
nothing. Under the supervisor, the death of any service stops the process, and the orchestrator
restarts it. The dev-mode parent watchdog and the Nuxt process stay in interloper-app
(section 2.4), as services handed to the same supervisor.

### 2.2 `interloper api` (interloper-api)

The API is served by `ApiServer`, a service wrapping `uvicorn.Server`:
- `start()` runs the server;
- `stop()` sets `should_exit`.

`ApiServer.from_settings(settings, *, store, catalog, port, cors_origins=None)` builds the FastAPI
app with `create_app`, instruments it, and configures uvicorn exactly as `_build_api` does today.
`interloper api` never serves the SPA: the api image has no `interloper-app`, and nginx serves the
frontend in containers.

The command:
- **Flags:** `--host`, `--port` and `--no-create-tables`, the last applied through
  `provision.bootstrap` (section 3).
- **Behaviour:** it bootstraps the database, builds the catalog and store, and supervises the one
  `ApiServer`.

### 2.3 `interloper scheduler` (interloper-scheduler)

The scheduler's services are built by
`SchedulerServices.from_settings(settings, *, store, catalog, cron, worker, reaper)`, which returns
the enabled services:
- cron, which brings hooks and renewal (renewal only when `renewal.enabled`);
- the queue worker and the reaper, sharing one `Launcher`.

That is today's `_build_cron` and `_build_launcher_services`, unchanged.

The command:
- **Flags:** `--cron/--no-cron`, `--worker/--no-worker`, `--reaper/--no-reaper` and
  `--no-create-tables`. Each toggle defaults to its settings section's `enabled` value, as `app`'s
  do today.
- **Behaviour:** it bootstraps the database and supervises the enabled services. With none enabled
  it exits with an error rather than idling.

### 2.4 `interloper app` (interloper-app)

`interloper app` is the all-in-one: the Makefile, local development and single-process
deployments. It keeps every flag it has today: `--api/--no-api`, `--cron/--no-cron`,
`--worker/--no-worker`, `--reaper/--no-reaper`, `--host`, `--port`, `--dev` and
`--no-create-tables`.

It composes the role services and supervises them together:
- an `ApiServer` with the built SPA mounted, outside dev mode;
- the `SchedulerServices`;
- in dev mode, the Nuxt dev server and the parent-death watchdog.

What moves from `runtime.py` into `interloper_app` (`services.py`, `assets.py`):
- **The API port choice** (`resolve_api_port`, `_find_free_port`) is app-only, because only dev
  mode moves the API off `server.port`. It becomes a method of the app's composition, not a free
  function.
- **The Nuxt dev process management:** occupied-port check, process-group kill, SIGKILL after the
  grace period.
- **The SPA fallback middleware** becomes a named ASGI class next to `static_dir()` in
  `interloper_app/assets.py`, instead of a class nested inside a method. Its `/api` prefix check
  matches the segment (`/api` or `/api/...`), not any path starting with `/api`.

### 2.5 Images, chart and Makefile

| Where | Before | After |
|---|---|---|
| dockerfile `api`, `api-slim` CMD | `interloper app --api --no-cron --no-worker --no-reaper --no-create-tables` | `interloper api --no-create-tables` |
| dockerfile `scheduler`, `scheduler-slim` CMD | `interloper app --no-api --cron --worker --reaper --no-create-tables` | `interloper scheduler --no-create-tables` |
| chart `api-deployment.yaml` args | `app --api --no-cron --no-worker --no-reaper --no-create-tables` | `api --no-create-tables` |
| chart `scheduler-deployment.yaml` args | `app --no-api --cron --worker --reaper --no-create-tables` | `scheduler --no-create-tables` |
| Makefile `dev-up`, compose | `interloper app --api --cron --worker --reaper --dev` | unchanged |

The chart and the images are released together under one version (the chart's image tag defaults
to its app version). A deployment that pins an older image under the new chart would run a command
that image lacks, so the upgrade note says to move both together.

## 3. `db` and `launch` move

`interloper db` moves to `interloper_db/cli.py` and `interloper launch` to
`interloper_scheduler/cli.py`. Their names, subcommands and flags are unchanged, including the
chart's `interloper db init` job and the launchers' `interloper launch <uuid>`.

The database bootstrap written three times (`app`, `db init`, `db reset`: `ensure_database`, then
`init_engine`, then `create_all`) becomes one procedure in `interloper_db/provision.py`:

```python
def bootstrap(dsn: str, *, statement_timeout: int | None = None, create_tables: bool = True) -> Engine:
    """Ensure the database exists, bind the engine, and create the tables unless asked not to."""
```

`db init` calls `bootstrap(dsn)` and then promotes the configured super-admins, as today. `db reset`
calls `drop_database(dsn)` then `bootstrap(dsn)`. The serving commands call it with their
`statement_timeout` and `create_tables=not args.no_create_tables`.

## 4. Platform errors move to `interloper-db`

`NotFoundError`, `ConflictError`, `InUseError`, `QuotaExceededError`, `HydrationError` and
`ComponentDriftError` move to `interloper_db/errors.py`, re-exported from `interloper_db`.

- Only `interloper-db` raises them, plus one `NotFoundError` in the scheduler's executor.
- Only db, api, toolkit and scheduler catch them, all of which depend on `interloper-db`.
- They still subclass core's `InterloperError`, so `except InterloperError` keeps catching them.
- The api's exception handlers keep their status codes and bodies.
- `CatalogKeyError` stays in core, which raises it. It and `ComponentDriftError` are kept apart on
  purpose: the api answers a bad key in a request with 400 and a stored row that drifted from the
  catalog with 409.

**What changes for consumers:**
- About 50 import sites update.
- `docs/reference/errors.md` gains `::: interloper_db.errors`.
- `docs/ui/definitions.md`'s link follows.
- Importing them from `interloper.errors` is an `ImportError`. Core cannot re-export them without
  importing db, so the upgrade skill gets the row.

**What remains in core's `errors.py` is tidied:**
- the module docstring stops claiming every error also inherits a built-in (five don't);
- the empty `# -- Source` divider goes;
- `DestinationError` and `DataNotFoundError` get their own `Destination` divider;
- `ConfigError`'s docstring describes what it means in core, not in the store and the api;
- `AssetNotFoundError` gets the `__str__` that `NotFoundError` has, so `str(error)` is not wrapped
  in `KeyError` quotes.

`format_exception` stays in `errors.py`, its owner as a formatter of exceptions for messages.

## 5. Bugs

Each bug gets a test that fails first.

| Bug | Fix |
|---|---|
| `db init` and `db reset` print the full DSN, password included | Messages name the target as `PostgresSettings.location` (`host:port/database`), a new property; the DSN is never printed. |
| `PostgresSettings.dsn` does not URL-escape user or password, so `@`, `:` or `/` in a password breaks the connection | Both are percent-encoded when the DSN is built (in core's `settings.py`, which stays). |
| `launch` leaves a run `dispatched` when catalog loading fails (`Catalog.from_settings` runs outside the `try` that marks the run failed) | The store opens first, since nothing can be recorded without it; everything after it runs inside the `try`. |
| `run -f spec.yaml --format inline` ignores `--format` | `-f` and `--format` become one mutually exclusive argparse group. The runners call `run --format inline <json>` without `-f`, so they are unaffected. |
| `app --dev --no-api` gives Nuxt its own port as the API port | `--dev` without the API is refused: the dev server only proxies to an API. |
| A bare `interloper app` without the api package crashes with an `ImportError` traceback | Gone by design: `app` lives in interloper-app, which depends on api and scheduler. |

## 6. Cleanup in the code that moves

- **`run.py`** (stays in core):
  - `_dag_from_paths` becomes `DAG.from_paths(paths)`, the example rule 8 uses.
  - `_print_plan` loses its always-empty `name` parameter.
  - `_resolve_window` stays as flag handling.
- **Names:** `db_sub`, `uvi_config`, `addr`, `sock`, `e`, `exc` and the `_DAG` alias are written
  out. `dsn` stays: it is the established term and the property's name.
- **Errors:** `app` raises `SystemExit("Error: ...")` like the other commands, not `print` plus
  `sys.exit(1)`.
- **Logging:** no f-strings in logging calls.
- **Stale docstrings:**
  - `app`'s says it omits the reaper and that db is "and/or";
  - `launch`'s promises failure handling it lacked;
  - `run`'s says "manifest" where everything else says "spec" and lists `Destination` as a target;
  - the `requires` vocabulary disappears with the machinery.

## 7. Contracts

**Unchanged:**
- `interloper run [--format inline] [--run-id ...] ...`;
- `interloper launch <uuid>`;
- `interloper db init|reset|upgrade|downgrade` and their flags;
- `interloper app` and every flag it has;
- the `interloper-mcp` script;
- every settings key and `INTERLOPER_*` env name;
- the api's error responses.

**Changed, with an upgrade-skill row:**
- the api and scheduler images' CMD and the chart's args move to `interloper api` and
  `interloper scheduler` (section 2.5);
- the platform errors import from `interloper_db`;
- a command whose package is not installed is now absent rather than refused by `requires`.

## 8. Testing

Unit tests only, mirroring the layout.

- **Moved tests follow their code:**
  - `tests/cli/test_runtime.py` is split into `interloper-app/tests/test_services.py` and
    `test_assets.py`, `interloper-api/tests/test_cli.py` and `interloper-scheduler/tests/test_cli.py`;
  - the `db` and `launch` command tests move to their packages.
- **The parser:**
  - with every workspace package installed, `build_parser()` lists `api`, `app`, `db`, `launch`,
    `run` and `scheduler`;
  - an entry point that fails to import stops the CLI with its error rather than silently
    vanishing.
- **The supervisor** (core):
  - stops every service on a signal;
  - stops the rest when one service's thread dies.
- **The role commands build the expected services from flags and settings**, without starting a
  server:
  - `scheduler --no-worker` builds cron (with hooks and renewal) and the reaper;
  - `app --no-api --dev` is refused;
  - `api` builds one `ApiServer` on `--port`.
- **`provision.bootstrap`** honours `create_tables=False`.
- **Errors:** the six errors import from `interloper_db` and are `InterloperError`s;
  `str(AssetNotFoundError("x"))` is `x`.
- **Each bug** in section 5 has its own test.
- **Full check:** `make check-python`, the strict docs build (CLI reference regenerated, errors
  reference) and the catalog dump against the base.
- **Live:**
  - the dev instance through `make dev-up` (exercises `interloper app` with every service);
  - `interloper api` and `interloper scheduler` started from the workspace on a spare port against
    the dev database: the API answers `/api/health`, a run of the demo source completes through
    the scheduler's worker, and SIGTERM stops each cleanly;
  - `interloper db upgrade` against the dev database;
  - the api and scheduler images built, each started with its new CMD far enough to print its
    `--help`.
