# Core phase 6a: platform out of core, implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every platform command and platform error lives in the package that owns it. Each role
starts through its own command (`interloper api`, `interloper scheduler`). The CLI bugs the surveys
found are fixed.

**Architecture:**
- Commands are contributed through one entry-point group, `interloper.cli`, read through core's
  existing `Registry`. Core's `main()` keeps only the lifecycle every command shares.
- A small type-agnostic `Supervisor` in `interloper.utils` runs any mapping of start/stop services in
  threads until a signal or a service death.
- `interloper-api` contributes an `ApiServer` service and the `api` command; `interloper-scheduler`
  contributes the scheduler services and the `scheduler` and `launch` commands.
- `interloper-app` composes both for `interloper app`, with the SPA and the Nuxt dev server.
  `interloper-db` gets the `db` command, one `bootstrap` procedure and the platform errors.

**Tech Stack:**
- Python 3.12, argparse, `importlib.metadata` entry points (through `interloper.registry.Registry`);
- uvicorn and FastAPI (api), SQLAlchemy and Alembic (db);
- pytest, ruff, ty, uv workspace, zensical docs.

**Spec:** `docs/superpowers/specs/2026-10-14-core-platform-commands-design.md`

## Global Constraints

- **Contracts that stay unchanged:**
  - **Core commands:** `interloper run [--format inline] [--run-id ...] ...`,
    `interloper launch <uuid>`, `interloper db init|reset|upgrade|downgrade` (with `--yes`/`-y` and
    the revision positionals);
  - **Serving and scripts:** `interloper app` with every flag it has today (`--host`, `--port`,
    `--api/--no-api`, `--cron/--no-cron`, `--worker/--no-worker`, `--reaper/--no-reaper`, `--dev`,
    `--no-create-tables`); the `interloper-mcp` script;
  - **Settings and errors:** every settings key and `INTERLOPER_*` env name; the api's error status
    codes and JSON bodies.
- **Settings stay one typed document in core (`interloper.settings.AppSettings`).** No section moves.
- **Out of scope (6b):** console logging setup (`configure_console_logging` calls and format
  strings), the `INTERLOPER_EVENTS_TO_STDERR` check and the telemetry lifecycle. These move as they
  are.
- **The six platform errors move to `interloper_db/errors.py`**, re-exported from `interloper_db`:
  `NotFoundError`, `ConflictError`, `InUseError`, `QuotaExceededError`, `HydrationError`,
  `ComponentDriftError`. They still subclass `interloper.errors.InterloperError`.
  `CatalogKeyError` stays in core.
- **House style** is in `.claude/rules/python-style.md`:
  - explicit names;
  - logic owned by its class;
  - constructors as `X.from_*` classmethods;
  - complete Google docstrings (`Args:`, `Returns:`, `Raises:`), sparse comments, no `#:`;
  - section dividers dash-filled to column 80.
- **Tests mirror the source layout**: a test for `src/<pkg>/<module>.py` lives in
  `tests/test_<module>.py` (or `tests/<pkg>/test_<module>.py` in core). No standalone
  feature test files.
- **Commands:**
  - **Check:** `UV_FROZEN=1 make check-python`.
  - **Sync after any `pyproject.toml` entry-point edit:**
    `UV_FROZEN=1 uv sync --all-packages --all-extras`, since entry points are installed metadata.
  - **Docs:** `UV_FROZEN=1 uv run zensical build --strict`.
- **Commit messages:** Conventional Commits, ending with the line `By Digitl`. Per the user's
  standing rule, nothing is committed during the tasks: the plan ends with one commit after the final
  review.

## Review Focus

1. **A service that fails at once** (the API's port already bound, a controller raising in its
   first tick loop): the process should exit non-zero within the grace period, naming the service.
   It should not hang or exit 0. Pinned in Task 1
   (`test_a_service_raising_on_start_stops_the_process_with_failure`).
2. **A broken plugin package** (an `interloper.cli` entry point whose module fails to import): the
   CLI should fail with that package's error, not show a parser silently missing the command.
   Pinned in Task 2 (`test_a_broken_command_entry_point_fails_loudly`).
3. **A password containing `@`, `:`, `/` or `%`** should connect, and the CLI should never print it.
   Pinned in Task 3 (`test_dsn_escapes_user_and_password`) and Task 4
   (`test_init_never_prints_the_password`).
4. **`interloper scheduler` with every service disabled** (`--no-cron --no-worker --no-reaper`, or
   all three disabled in settings) should exit with an error instead of idling. Pinned in Task 6
   (`test_no_enabled_service_is_an_error`).
5. **SIGTERM delivered to `interloper api` or `interloper scheduler`** should stop every service
   and exit 0, as the pods' termination relies on it. Pinned in Task 1
   (`test_sigterm_stops_every_service_and_returns`).

---

## File structure

**Created:**
- `packages/interloper-core/src/interloper/utils/supervisor.py`: `Service` protocol and `Supervisor`.
- `packages/interloper-db/src/interloper_db/cli.py`: the `db` command.
- `packages/interloper-db/src/interloper_db/errors.py`: the six platform errors.
- `packages/interloper-scheduler/src/interloper_scheduler/services.py`: `scheduler_services(...)`.
- `packages/interloper-scheduler/src/interloper_scheduler/cli.py`: the `scheduler` and `launch`
  commands.
- `packages/interloper-api/src/interloper_api/server.py`: `ApiServer`.
- `packages/interloper-api/src/interloper_api/cli.py`: the `api` command.
- `packages/interloper-app/src/interloper_app/services.py`: `App` (composition), `DevServer`,
  `ParentWatchdog`.
- `packages/interloper-app/src/interloper_app/cli.py`: the `app` command.

**Deleted:**
- `packages/interloper-core/src/interloper/cli/runtime.py`;
- `packages/interloper-core/src/interloper/cli/commands/{app,db,launch}.py`;
- their tests under `packages/interloper-core/tests/cli/` (`test_runtime.py`, `commands/test_app.py`,
  `commands/test_db.py`, `commands/test_launch.py`), ported to the owning packages.

**Modified:**
- **core:**
  - `cli/main.py`, `cli/commands/run.py`;
  - `utils/__init__.py`, `settings.py` (PostgresSettings only), `errors.py`, `dag/base.py`
    (`DAG.from_paths`);
  - `pyproject.toml` (entry points).
- **The other packages' `pyproject.toml`**, each gaining entry points.
- **About 50 import sites** of the platform errors (api, db, scheduler, toolkit).
- **Build and deployment:** `dockerfile`, `chart/interloper/templates/{api,scheduler}-deployment.yaml`.
- **Docs:** `dev/docs_macros.py`, `docs/guide/cli.md`, `docs/ui/running.md`, `docs/ui/definitions.md`,
  `docs/reference/errors.md`.
- **Plugin skills:** `plugins/interloper/skills/{interloper-deploy,interloper-triage,interloper-upgrade}/SKILL.md`,
  `README.md`.

---

### Task 1: The supervisor

**Files:**
- Create: `packages/interloper-core/src/interloper/utils/supervisor.py`
- Modify: `packages/interloper-core/src/interloper/utils/__init__.py` (export `Service`, `Supervisor`)
- Test: `packages/interloper-core/tests/utils/test_supervisor.py`

**Interfaces:**
- Produces:
  - `interloper.utils.Service`, a `typing.Protocol` with `start() -> None` (blocking) and
    `stop() -> None`;
  - `interloper.utils.Supervisor(services: Mapping[str, Service], *, grace: float = 5.0)`, with
    `run() -> None` (blocks; raises `SystemExit(1)` when stopped by a service's death) and
    `stop(reason: str) -> None` (idempotent, thread-safe).

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for ``interloper.utils.supervisor``."""

from __future__ import annotations

import os
import signal
import threading
import time

import pytest

from interloper.utils import Supervisor


class Loop:
    """A service that loops until stopped, recording its lifecycle."""

    def __init__(self) -> None:
        self.stopped = threading.Event()
        self.started = threading.Event()

    def start(self) -> None:
        self.started.set()
        self.stopped.wait()

    def stop(self) -> None:
        self.stopped.set()


class Exits:
    """A service whose blocking call returns on its own."""

    def start(self) -> None:
        return

    def stop(self) -> None:
        return


class Raises:
    """A service whose blocking call raises."""

    def start(self) -> None:
        raise OSError("address already in use")

    def stop(self) -> None:
        return


def test_stop_stops_every_service_and_returns():
    """``stop`` ends ``run`` with every service stopped."""
    first, second = Loop(), Loop()
    supervisor = Supervisor({"first": first, "second": second}, grace=1.0)
    threading.Timer(0.2, supervisor.stop, args=["test"]).start()

    supervisor.run()

    assert first.stopped.is_set() and second.stopped.is_set()


def test_sigterm_stops_every_service_and_returns():
    """SIGTERM is a clean shutdown: every service stops and ``run`` returns normally."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop}, grace=1.0)
    threading.Timer(0.2, os.kill, args=[os.getpid(), signal.SIGTERM]).start()

    supervisor.run()

    assert loop.stopped.is_set()


def test_a_service_exiting_stops_the_rest_and_fails():
    """A service that returns on its own takes the process down, with a failure status."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop, "exits": Exits()}, grace=1.0)

    with pytest.raises(SystemExit) as error:
        supervisor.run()

    assert error.value.code == 1
    assert loop.stopped.is_set()


def test_a_service_raising_on_start_stops_the_process_with_failure(caplog: pytest.LogCaptureFixture):
    """A service that raises is logged by name and fails the process, within the grace period."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop, "api": Raises()}, grace=1.0)

    started = time.monotonic()
    with pytest.raises(SystemExit):
        supervisor.run()

    assert time.monotonic() - started < 3
    assert any("api" in record.getMessage() for record in caplog.records)


def test_stop_is_idempotent():
    """A second ``stop`` is ignored."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop}, grace=1.0)
    threading.Timer(0.2, supervisor.stop, args=["first"]).start()
    threading.Timer(0.3, supervisor.stop, args=["second"]).start()

    supervisor.run()

    assert loop.stopped.is_set()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/utils/test_supervisor.py -q -p no:cacheprovider`
Expected: FAIL (`ImportError: cannot import name 'Supervisor'`).

- [ ] **Step 3: Implement**

`packages/interloper-core/src/interloper/utils/supervisor.py`:

```python
"""Run long-lived services in threads until a signal or a service's death."""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Mapping
from typing import Protocol

logger = logging.getLogger(__name__)


class Service(Protocol):
    """Something a `Supervisor` runs: a blocking ``start`` and a ``stop`` that ends it."""

    def start(self) -> None:
        """Run until stopped."""

    def stop(self) -> None:
        """Ask the running ``start`` to return."""


class Supervisor:
    """Run services in daemon threads and stop them all together.

    A signal (SIGINT, SIGTERM, SIGHUP) is a clean shutdown. A service whose
    ``start`` returns or raises while nothing asked it to is a death: every
    other service is stopped too and ``run`` fails, so an orchestrator
    restarts the process instead of keeping one that serves nothing.
    """

    def __init__(self, services: Mapping[str, Service], *, grace: float = 5.0) -> None:
        """Hold the services to run.

        Args:
            services: The services, by the name their thread and log lines carry.
            grace: Seconds each thread is given to finish once stopped.
        """
        self.services = dict(services)
        self.grace = grace
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._failed = False

    def run(self) -> None:
        """Start every service and block until they are stopped.

        Raises:
            SystemExit: With status 1 when a service died rather than being stopped.
        """
        if threading.current_thread() is threading.main_thread():
            self._install_signal_handlers()
        threads = [
            threading.Thread(target=self._run_service, args=(name, service), name=name, daemon=True)
            for name, service in self.services.items()
        ]
        for thread in threads:
            thread.start()
        logger.info("Started services: %s", ", ".join(self.services))
        while not self._stopping.wait(timeout=1):
            pass
        for thread in threads:
            thread.join(timeout=self.grace)
        logger.info("Shutdown complete.")
        if self._failed:
            raise SystemExit(1)

    def stop(self, reason: str) -> None:
        """Stop every service; later calls are ignored.

        Args:
            reason: Why the shutdown started, logged with it.
        """
        with self._lock:
            if self._stopping.is_set():
                return
            self._stopping.set()
        logger.info("Shutting down (%s)...", reason)
        for name, service in self.services.items():
            try:
                service.stop()
            except Exception:  # noqa: BLE001 - one service failing to stop must not keep the others running
                logger.exception("Service '%s' failed to stop", name)

    # -- Internals -------------------------------------------------------------

    def _run_service(self, name: str, service: Service) -> None:
        """Run one service, turning an unrequested return or an exception into a shutdown.

        Args:
            name: The service's name.
            service: The service.
        """
        try:
            service.start()
        except Exception:  # noqa: BLE001 - a dying service ends the process, logged by name
            logger.exception("Service '%s' failed", name)
            self._die(name)
            return
        if not self._stopping.is_set():
            logger.error("Service '%s' exited unexpectedly", name)
            self._die(name)

    def _die(self, name: str) -> None:
        """Record a service's death and stop the rest.

        Args:
            name: The service that died.
        """
        self._failed = True
        self.stop(f"service '{name}' died")

    def _install_signal_handlers(self) -> None:
        """Route SIGINT, SIGTERM and (where available) SIGHUP into ``stop``."""
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, self._on_signal)

    def _on_signal(self, signal_number: int, frame: object) -> None:
        """Stop, naming the received signal.

        Args:
            signal_number: The signal number delivered by the OS.
            frame: The interrupted stack frame; unused.
        """
        self.stop(f"signal {signal.Signals(signal_number).name}")
```

Add `from interloper.utils.supervisor import Service, Supervisor` to `utils/__init__.py`, and
`"Service"`, `"Supervisor"` to its `__all__` (kept sorted).

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/utils/test_supervisor.py -q -p no:cacheprovider`
Expected: PASS (5 passed). The SIGTERM test replaces pytest's handler for the rest of the session;
if it disturbs later tests, restore the previous handlers in a fixture (`signal.getsignal` before,
`signal.signal` after).

---

### Task 2: Commands come from entry points

**Files:**
- Modify: `packages/interloper-core/src/interloper/cli/main.py`, `packages/interloper-core/pyproject.toml`,
  `dev/docs_macros.py`
- Test: `packages/interloper-core/tests/cli/test_main.py`

**Interfaces:**
- Produces:
  - `interloper.cli.main.COMMANDS: Registry[Callable[[argparse._SubParsersAction], None]]`, over the
    `interloper.cli` group;
  - `build_parser()` registering every entry sorted by name;
  - `main()` without `requires` checks or the `app` special case.
- Produces for Tasks 4, 6, 7 and 8: a command contributes itself with
  `[project.entry-points."interloper.cli"] <name> = "<module>:<register function>"`.

- [ ] **Step 1: Write the failing tests**

Replace `TestHasPackage`, `TestEnforceRequires`, `test_app_command_folds_cli_overrides_into_settings`
and `test_unsatisfied_requirement_aborts_before_running` in `tests/cli/test_main.py` with:

```python
class TestCommands:
    """Commands are contributed through the ``interloper.cli`` entry-point group."""

    def test_the_parser_holds_every_installed_command_sorted(self) -> None:
        parser = build_parser()
        subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))

        assert list(subparsers.choices) == sorted(subparsers.choices)
        assert {"app", "db", "launch", "run"} <= set(subparsers.choices)

    def test_a_broken_command_entry_point_fails_loudly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def broken(_subparsers: Any) -> None:
            raise ImportError("interloper_broken needs a dependency that is not installed")

        monkeypatch.setattr(COMMANDS, "items", lambda: (("broken", broken),))

        with pytest.raises(ImportError, match="interloper_broken"):
            build_parser()

    def test_main_does_not_know_any_command(self) -> None:
        import inspect

        from interloper.cli import main as main_module

        source = inspect.getsource(main_module)
        assert "requires" not in source
        assert '"app"' not in source
```

(`COMMANDS` and `build_parser` are imported from `interloper.cli.main`; `argparse` and `Any` at
module top.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/cli/test_main.py -q -p no:cacheprovider`
Expected: FAIL (`ImportError: cannot import name 'COMMANDS'`).

- [ ] **Step 3: Implement**

In `packages/interloper-core/pyproject.toml`, add (the platform commands still point at core
modules; Tasks 4, 6 and 8 repoint them):

```toml
[project.entry-points."interloper.cli"]
app = "interloper.cli.commands.app:register"
db = "interloper.cli.commands.db:register"
launch = "interloper.cli.commands.launch:register"
run = "interloper.cli.commands.run:register"
```

Run `UV_FROZEN=1 uv sync --all-packages --all-extras`.

Rewrite `cli/main.py` to:

```python
"""Interloper CLI entry point: every command is contributed through the ``interloper.cli`` group."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from interloper.registry import Registry
from interloper.settings import AppSettings

COMMANDS: Registry[Callable[[argparse._SubParsersAction], None]] = Registry("interloper.cli")


def _load_dotenv() -> None:
    ...  # unchanged


def build_parser() -> argparse.ArgumentParser:
    """Assemble the ``interloper`` parser with every installed command, sorted by name.

    The documentation site renders the CLI reference from this parser, so
    every flag's ``help`` and every command's ``description`` is user-facing
    copy.

    Returns:
        The root parser.
    """
    parser = argparse.ArgumentParser(prog="interloper", description="Interloper CLI")
    subparsers = parser.add_subparsers(dest="command")
    for _name, register in sorted(COMMANDS.items()):
        register(subparsers)
    return parser


def main() -> None:
    """Run one command: load ``.env``, activate settings and telemetry, call the handler, shut down."""
    _load_dotenv()

    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    AppSettings.activate(AppSettings.get())

    from interloper.telemetry import init_telemetry, shutdown_telemetry

    init_telemetry(AppSettings.get().otel)
    try:
        args.handler(args)
    finally:
        shutdown_telemetry()
        AppSettings.clear_active()
```

The handlers that need the override live with their commands. Until Task 8 moves `app`, add the
override at the top of `commands/app.py`'s `_cmd_app`, before it reads settings:

```python
    AppSettings.activate(apply_cli_overrides(args, AppSettings.get()))
```

Remove every `requires=...`/`requires_when=...` keyword from the `set_defaults` calls in
`commands/{app,db,launch}.py`. In `dev/docs_macros.py`, delete the `requires`/`required_when` lines
and the `inherited` parameter from `_command`, and its use in `_subcommands`' caller.

- [ ] **Step 4: Run the tests and the docs build**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/cli -q -p no:cacheprovider`
Expected: PASS. The app tests that asserted `requires_when` (`test_declares_its_conditional_requirements`)
are deleted with the machinery.
Run: `UV_FROZEN=1 uv run zensical build --strict`
Expected: exit 0; `site/reference/cli/index.html` no longer contains "Requires".

---

### Task 3: The Postgres DSN escapes and never shows its password

**Files:**
- Modify: `packages/interloper-core/src/interloper/settings.py` (`PostgresSettings`)
- Test: `packages/interloper-core/tests/test_settings.py`

**Interfaces:**
- Produces:
  - `PostgresSettings.dsn`, with user and password percent-encoded;
  - `PostgresSettings.location -> str` (`"host:port/database"`, no credentials).

- [ ] **Step 1: Write the failing tests**

```python
def test_dsn_escapes_user_and_password():
    """Reserved characters in the credentials do not break the connection string."""
    settings = PostgresSettings(user="me@corp", password="p@ss:w/rd%", host="db", port=5432, database="interloper")

    assert settings.dsn == "postgresql://me%40corp:p%40ss%3Aw%2Frd%25@db:5432/interloper"


def test_location_names_the_target_without_credentials():
    """``location`` is what a message may show: host, port and database."""
    settings = PostgresSettings(user="me", password="secret", host="db", port=5433, database="warehouse")

    assert settings.location == "db:5433/warehouse"
    assert "secret" not in settings.location
```

(`PostgresSettings` from `interloper.settings`.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/test_settings.py -q -p no:cacheprovider -k "dsn or location"`
Expected: FAIL (unescaped DSN; `location` missing).

- [ ] **Step 3: Implement**

```python
    @property
    def dsn(self) -> str:
        """The PostgreSQL connection string, user and password percent-encoded.

        Returns:
            ``postgresql://user:password@host:port/database``.
        """
        credentials = f"{quote(self.user, safe='')}:{quote(self.password, safe='')}"
        return f"postgresql://{credentials}@{self.host}:{self.port}/{self.database}"

    @property
    def location(self) -> str:
        """The database this connects to, safe to print: no credentials.

        Returns:
            ``host:port/database``.
        """
        return f"{self.host}:{self.port}/{self.database}"
```

with `from urllib.parse import quote` at the top of `settings.py`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/test_settings.py packages/interloper-db -q -p no:cacheprovider`
Expected: PASS (the db engine tests build engines from `dsn`; a credential without reserved
characters is unchanged by `quote`).

---

### Task 4: `db` lives in `interloper-db`, with one bootstrap

**Files:**
- Create: `packages/interloper-db/src/interloper_db/cli.py`
- Modify: `packages/interloper-db/src/interloper_db/provision.py` (add `bootstrap`),
  `packages/interloper-db/pyproject.toml`, `packages/interloper-core/pyproject.toml` (drop the `db` entry)
- Delete: `packages/interloper-core/src/interloper/cli/commands/db.py`
- Test: `packages/interloper-db/tests/test_provision.py` (bootstrap), `packages/interloper-db/tests/test_cli.py`
  (ported from `packages/interloper-core/tests/cli/commands/test_db.py`, which is deleted)

**Interfaces:**
- Consumes: Task 2's entry-point contract; Task 3's `PostgresSettings.location`.
- Produces: `interloper_db.provision.bootstrap(dsn: str, *, statement_timeout: float | None = None,
  create_tables: bool = True) -> Engine`; the `db` command at `interloper_db.cli:register`.

- [ ] **Step 1: Write the failing tests**

In `packages/interloper-db/tests/test_provision.py`:

```python
class TestBootstrap:
    """One procedure brings a database up: ensure it, bind the engine, create the tables."""

    def test_ensures_binds_and_creates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[str, Any]] = []
        engine = object()
        monkeypatch.setattr(provision, "ensure_database", lambda dsn: calls.append(("ensure", dsn)))
        monkeypatch.setattr(
            provision, "init_engine", lambda dsn, statement_timeout=None: calls.append(("init", statement_timeout)) or engine
        )
        monkeypatch.setattr(provision, "create_all", lambda bound: calls.append(("create", bound)))

        assert provision.bootstrap("postgresql://db/x", statement_timeout=30) is engine
        assert calls == [("ensure", "postgresql://db/x"), ("init", 30), ("create", engine)]

    def test_create_tables_false_skips_the_tables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        created: list[Any] = []
        monkeypatch.setattr(provision, "ensure_database", lambda dsn: None)
        monkeypatch.setattr(provision, "init_engine", lambda dsn, statement_timeout=None: object())
        monkeypatch.setattr(provision, "create_all", created.append)

        provision.bootstrap("postgresql://db/x", create_tables=False)

        assert created == []
```

(`provision` is `interloper_db.provision`; `Any` from `typing`.)

In `packages/interloper-db/tests/test_cli.py`, port every test of `tests/cli/commands/test_db.py`
with these rewrites:
- **Imports:** `from interloper.cli.commands import db as db_command` becomes
  `from interloper_db import cli as db_command`.
- **Patch targets:** the fixture patches `interloper_db.cli.bootstrap`, `interloper_db.cli.drop_database`,
  `interloper_db.cli.upgrade`, `interloper_db.cli.downgrade`, `interloper_db.cli.init_engine` and
  `interloper_db.cli.Store`. The new module imports these names at its top.
- **Dropped:** `test_every_subcommand_declares_the_db_requirement`, since `requires` is gone.

Add:

```python
def test_init_never_prints_the_password(calls: dict[str, list[Any]], capsys: pytest.CaptureFixture[str],
                                         monkeypatch: pytest.MonkeyPatch) -> None:
    """``db init`` names its target by location; the password never reaches the output."""
    settings = AppSettings(postgres=PostgresSettings(user="me", password="s3cret-pw", host="db", database="x"))
    monkeypatch.setattr(AppSettings, "_active", settings)

    db_command._cmd_init(argparse.Namespace())

    output = capsys.readouterr().out
    assert "s3cret-pw" not in output
    assert "db:5432/x" in output
```

and the same for `_cmd_reset` with `argparse.Namespace(yes=True)`.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/test_provision.py packages/interloper-db/tests/test_cli.py -q -p no:cacheprovider`
Expected: FAIL (`bootstrap` missing; `interloper_db.cli` missing).

- [ ] **Step 3: Implement**

In `provision.py`:

```python
def bootstrap(dsn: str, *, statement_timeout: float | None = None, create_tables: bool = True) -> Engine:
    """Bring a database up: ensure it exists, bind the process engine, create the tables.

    ``create_all`` is idempotent and serialized by an advisory lock, so
    concurrent bootstraps are safe; production deploys run ``interloper db
    init`` once and start the serving commands with ``create_tables=False``.

    Args:
        dsn: The PostgreSQL connection string.
        statement_timeout: Seconds after which the server cancels a statement; ``None`` keeps the server's setting.
        create_tables: Whether to create the tables and run the migrations.

    Returns:
        The bound engine.
    """
    from interloper_db.engine import init_engine

    ensure_database(dsn)
    engine = init_engine(dsn, statement_timeout=statement_timeout)
    if create_tables:
        create_all(engine)
    return engine
```

If `init_engine` is already importable at module top without a cycle, import it there instead.
`provision` imports `get_engine` from `interloper_db.engine` today, so it is.

Create `interloper_db/cli.py`:
- Move `register`, `_cmd_init`, `_cmd_reset`, `_cmd_upgrade` and `_cmd_downgrade` from core's
  `commands/db.py`, with module-top imports (`AppSettings`, `Catalog`, `Store`, `bootstrap`,
  `drop_database`, `init_engine`, `upgrade`, `downgrade`).
- `register`: `db_sub` becomes `db_subparsers`, no `requires` defaults. The bare-`db` handler stays
  `lambda args: db_parser.print_help()`.
- `_cmd_init`:

  ```python
  postgres = AppSettings.get().postgres
  print(f"Bootstrapping the database at {postgres.location}...")
  engine = bootstrap(postgres.dsn)
  store = Store(catalog=Catalog(components={}), engine=engine)
  ```

  followed by the super-admin promotion and `print("Done.")` as today.
- `_cmd_reset`: the prompt reads `f"This will DROP and recreate the database at {postgres.location}. Continue? [y/N] "`.
  It then calls `drop_database(postgres.dsn)` and `bootstrap(postgres.dsn)`.
- `_cmd_upgrade` and `_cmd_downgrade` are unchanged apart from the module-top imports.
- The module docstring loses "Available when `interloper-db` is installed".

In `packages/interloper-db/pyproject.toml`:

```toml
[project.entry-points."interloper.cli"]
db = "interloper_db.cli:register"
```

Remove the `db` line from core's `interloper.cli` group, delete `cli/commands/db.py` and
`tests/cli/commands/test_db.py`, and run `UV_FROZEN=1 uv sync --all-packages --all-extras`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db packages/interloper-core/tests/cli -q -p no:cacheprovider`
Expected: PASS; `interloper db --help` (via `UV_FROZEN=1 uv run interloper db --help`) lists the four
subcommands.

---

### Task 5: The platform errors live in `interloper-db`

**Files:**
- Create: `packages/interloper-db/src/interloper_db/errors.py`
- Modify:
  - `packages/interloper-db/src/interloper_db/__init__.py`;
  - `packages/interloper-core/src/interloper/errors.py`;
  - every import site (find them with
    `grep -rnE "\b(NotFoundError|ConflictError|InUseError|QuotaExceededError|HydrationError|ComponentDriftError)\b" packages --include='*.py'`);
  - `docs/reference/errors.md`, `docs/ui/definitions.md`.
- Test: `packages/interloper-db/tests/test_errors.py` (new);
  `packages/interloper-core/tests/test_errors.py` (its platform-error tests move to the db test).

**Interfaces:**
- Produces: `from interloper_db import NotFoundError, ConflictError, InUseError, QuotaExceededError,
  HydrationError, ComponentDriftError`, each a subclass of `interloper.errors.InterloperError` with
  today's bases and bodies.

- [ ] **Step 1: Write the failing tests**

`packages/interloper-db/tests/test_errors.py`:

```python
"""Tests for ``interloper_db.errors``."""

import importlib

import pytest

import interloper_db
from interloper.errors import InterloperError

PLATFORM_ERRORS = ["NotFoundError", "ConflictError", "InUseError", "QuotaExceededError", "HydrationError",
                   "ComponentDriftError"]


@pytest.mark.parametrize("name", PLATFORM_ERRORS)
def test_the_platform_errors_live_in_the_db_package(name: str) -> None:
    """Each platform error is exported by ``interloper_db`` and is still an ``InterloperError``."""
    error_class = getattr(interloper_db, name)

    assert issubclass(error_class, InterloperError)
    assert not hasattr(importlib.import_module("interloper.errors"), name)
```

Then move the existing tests of these six classes from core's `tests/test_errors.py`, e.g. the
`NotFoundError` `str` test and the `QuotaExceededError` fields test, into this file, importing from
`interloper_db`.

In core's `tests/test_errors.py`, add:

```python
def test_asset_not_found_reads_without_key_error_quotes():
    """``str`` of an ``AssetNotFoundError`` is its message, not ``KeyError``'s quoted repr."""
    assert str(AssetNotFoundError("Asset 'x' not found")) == "Asset 'x' not found"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-db/tests/test_errors.py packages/interloper-core/tests/test_errors.py -q -p no:cacheprovider`
Expected: FAIL (the classes are not on `interloper_db`; `AssetNotFoundError` str is quoted).

- [ ] **Step 3: Implement**

- **Move the six classes:**
  - Cut them, with their divider blocks' docstrings and `__str__`/`__init__` bodies, from
    `interloper/errors.py` into `interloper_db/errors.py`.
  - Give the new module the docstring "Errors the platform's persistence raises and its services map
    to responses."
  - Import `InterloperError` (and `ConfigError` if a base needs it) from `interloper.errors`.
  - Export the six from `interloper_db/__init__.py` and its `__all__`.
- **Update every import site** from the grep in **Files**: `from interloper.errors import NotFoundError`
  becomes `from interloper_db import NotFoundError`, and so on. Where core names stay in the same
  import line (`ConfigError`, `CatalogKeyError`, ...), split the line in two. Inside `interloper-db`
  itself, import from `interloper_db.errors` to avoid a cycle with `interloper_db/__init__.py`.
- **Tidy core's `errors.py`:**
  - the module docstring no longer claims every error also inherits a built-in;
  - delete the empty `# -- Source` divider;
  - give `DestinationError` and `DataNotFoundError` a `# -- Destination` divider, dash-filled to
    column 80;
  - `ConfigError`'s docstring describes it in core terms ("A component, spec or setting is
    misconfigured."), with no store or api;
  - delete the now-empty `# -- Lookup / Not Found` and `# -- Hydration / Catalog` dividers, keeping
    `CatalogKeyError` under a `# -- Catalog` divider;
  - add to `AssetNotFoundError`:

    ```python
        def __str__(self) -> str:
            """The message, without the quotes ``KeyError`` adds.

            Returns:
                The error message.
            """
            return str(self.args[0]) if self.args else ""
    ```
- **Docs:** add `::: interloper_db.errors` to `docs/reference/errors.md`, after `::: interloper.errors`,
  and update the link in `docs/ui/definitions.md` to the new anchor.

- [ ] **Step 4: Run the tests and the type check**

Run: `UV_FROZEN=1 uv run ty check --output-format concise 2>&1 | grep -E "error|warning" | head`
Expected: no output.
Run: `UV_FROZEN=1 uv run pytest packages -q -p no:cacheprovider > /tmp/t5.log 2>&1; tail -1 /tmp/t5.log`
Expected: all passed.

---

### Task 6: `scheduler` and `launch` live in `interloper-scheduler`

**Files:**
- Create: `packages/interloper-scheduler/src/interloper_scheduler/services.py`, `.../cli.py`
- Modify: `packages/interloper-scheduler/pyproject.toml`, `packages/interloper-scheduler/src/interloper_scheduler/__init__.py`,
  `packages/interloper-core/pyproject.toml` (drop `launch`)
- Delete: `packages/interloper-core/src/interloper/cli/commands/launch.py`
- Test: `packages/interloper-scheduler/tests/test_services.py`, `packages/interloper-scheduler/tests/test_cli.py`
  (ports `packages/interloper-core/tests/cli/commands/test_launch.py`, deleted)

**Interfaces:**
- Consumes: Task 1's `Supervisor`/`Service`; Task 2's entry-point contract; Task 4's `bootstrap`.
- Produces:
  - `interloper_scheduler.scheduler_services(settings: AppSettings, *, store: Store, catalog: Catalog,
    cron: bool, worker: bool, reaper: bool) -> dict[str, Service]`. Its keys are `cron`, `hooks`,
    `renewal` (when `settings.renewal.enabled`), `worker` and `reaper`, each present when enabled.
    A function rather than the spec's `SchedulerServices.from_settings`, since it builds a plain
    mapping it does not own (rule 8's exception).
  - The commands `scheduler` (`interloper_scheduler.cli:register_scheduler`) and `launch`
    (`interloper_scheduler.cli:register_launch`).

- [ ] **Step 1: Write the failing tests**

`tests/test_services.py`:

```python
"""Tests for ``interloper_scheduler.services``."""

from typing import Any

import pytest

import interloper_scheduler.services as services_module
from interloper.catalog import Catalog
from interloper.settings import AppSettings, RenewalSettings
from interloper_scheduler import scheduler_services


@pytest.fixture(autouse=True)
def _no_launcher(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(services_module.Launcher, "from_settings", classmethod(lambda cls, *a, **k: object()))


def _build(**toggles: bool) -> dict[str, Any]:
    return scheduler_services(AppSettings(), store=object(), catalog=Catalog(), **toggles)  # ty: ignore[invalid-argument-type]


def test_cron_brings_hooks_and_renewal():
    assert set(_build(cron=True, worker=False, reaper=False)) == {"cron", "hooks", "renewal"}


def test_renewal_follows_its_setting():
    settings = AppSettings(renewal=RenewalSettings(enabled=False))
    built = scheduler_services(settings, store=object(), catalog=Catalog(), cron=True, worker=False, reaper=False)  # ty: ignore[invalid-argument-type]

    assert set(built) == {"cron", "hooks"}


def test_worker_and_reaper_share_one_launcher():
    built = _build(cron=False, worker=True, reaper=True)

    assert set(built) == {"worker", "reaper"}
    assert built["worker"]._launcher is built["reaper"]._launcher
```

(Adjust `_launcher` to the attribute name `QueueController` and `Reaper` store their launcher
under; read `queue.py`/`reaper.py`.)

`tests/test_cli.py`:

```python
"""Tests for ``interloper_scheduler.cli``."""

import argparse
from typing import Any

import pytest

from interloper.settings import AppSettings, CronSettings, ReaperSettings, WorkerSettings
from interloper_scheduler import cli


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    cli.register_scheduler(subparsers)
    cli.register_launch(subparsers)
    return parser.parse_args(argv)


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(cli, "bootstrap", lambda dsn, **kwargs: recorded.setdefault("bootstrap", kwargs))
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(cli, "scheduler_services",
                        lambda settings, **kwargs: recorded.setdefault("services", kwargs) and {"stub": object()})
    monkeypatch.setattr(cli.Supervisor, "run", lambda self: recorded.setdefault("ran", list(self.services)))
    return recorded


def test_flags_override_the_settings(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(AppSettings, "_active", AppSettings())
    cli._cmd_scheduler(_parse("scheduler", "--no-worker"))

    assert wiring["services"]["cron"] is True
    assert wiring["services"]["worker"] is False


def test_no_enabled_service_is_an_error(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    settings = AppSettings(cron=CronSettings(enabled=False), worker=WorkerSettings(enabled=False),
                           reaper=ReaperSettings(enabled=False))
    monkeypatch.setattr(AppSettings, "_active", settings)

    with pytest.raises(SystemExit, match="No scheduler service enabled"):
        cli._cmd_scheduler(_parse("scheduler"))


def test_no_create_tables_reaches_the_bootstrap(wiring: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(AppSettings, "_active", AppSettings())
    cli._cmd_scheduler(_parse("scheduler", "--no-create-tables"))

    assert wiring["bootstrap"]["create_tables"] is False


def test_a_catalog_failure_marks_the_launched_run_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    completed: list[tuple[Any, bool]] = []

    class RecordingRuns:
        def complete(self, run_id: Any, *, success: bool) -> None:
            completed.append((run_id, success))

    def failing_catalog(cls: Any) -> None:
        raise ValueError("bad catalog setting")

    monkeypatch.setattr(AppSettings, "_active", AppSettings())
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(failing_catalog))
    monkeypatch.setattr(cli.Store, "from_settings",
                        classmethod(lambda cls, catalog=None: type("S", (), {"runs": RecordingRuns()})()))
    args = _parse("launch", "11111111-1111-1111-1111-111111111111")

    with pytest.raises(SystemExit):
        cli._cmd_launch(args)

    assert completed == [(args.run_id, False)]
```

Port the remaining tests of core's `tests/cli/commands/test_launch.py`, retargeting the patches to
`interloper_scheduler.cli`.

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-scheduler/tests/test_services.py packages/interloper-scheduler/tests/test_cli.py -q -p no:cacheprovider`
Expected: FAIL (modules missing).

- [ ] **Step 3: Implement**

`services.py`, built from today's `Services._build_cron` and `_build_launcher_services`:

```python
"""The scheduler's long-lived services, built from settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper_scheduler.cron import CronController
from interloper_scheduler.hooks import HookController
from interloper_scheduler.launcher import Launcher
from interloper_scheduler.queue import QueueController
from interloper_scheduler.reaper import Reaper
from interloper_scheduler.renewal import RenewalController

if TYPE_CHECKING:
    from interloper.catalog import Catalog
    from interloper.settings import AppSettings
    from interloper.utils import Service
    from interloper_db import Store


def scheduler_services(
    settings: AppSettings, *, store: Store, catalog: Catalog, cron: bool, worker: bool, reaper: bool
) -> dict[str, Service]:
    """Build the enabled scheduler services, by name.

    Cron brings the hook and renewal controllers, the cluster singletons that
    turn declared intent into runs; renewal also follows ``renewal.enabled``.
    The worker and the reaper share one launcher.

    Args:
        settings: The active settings.
        store: The store every service reads and writes through.
        catalog: The catalog the renewal controller resolves connections against.
        cron: Whether to run the cron, hook and renewal controllers.
        worker: Whether to run the queue worker.
        reaper: Whether to run the reaper.

    Returns:
        The services, keyed ``cron``, ``hooks``, ``renewal``, ``worker`` and ``reaper``, each present when enabled.
    """
    services: dict[str, Service] = {}
    if cron:
        services["cron"] = CronController(
            store=store,
            reconcile_interval=settings.cron.reconcile_interval,
            max_execution_delay=settings.cron.max_execution_delay,
            batch_size=settings.cron.batch_size,
        )
        services["hooks"] = HookController(store=store)
        if settings.renewal.enabled:
            services["renewal"] = RenewalController(
                catalog=catalog,
                store=store,
                reconcile_interval=settings.renewal.reconcile_interval,
                batch_size=settings.renewal.batch_size,
            )
    if worker or reaper:
        launcher = Launcher.from_settings(
            settings.launcher,
            postgres=settings.postgres,
            runner=settings.runner,
            reaper=settings.reaper,
            catalog=catalog,
            store=store,
        )
        if worker:
            services["worker"] = QueueController(
                launcher=launcher, store=store, poll_interval=settings.worker.poll_interval
            )
        if reaper:
            services["reaper"] = Reaper(
                store=store,
                launcher=launcher,
                startup_timeout=settings.reaper.startup_timeout,
                heartbeat_timeout=settings.reaper.heartbeat_timeout,
                run_timeout=settings.reaper.run_timeout,
                poll_interval=settings.reaper.poll_interval,
            )
    return services
```

Export `scheduler_services` from `interloper_scheduler/__init__.py` and its `__all__`.

`cli.py`:

```python
"""The scheduler's commands: ``interloper scheduler`` runs its services, ``interloper launch`` one run."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from uuid import UUID

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.runner import Runner
from interloper.settings import AppSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap
from interloper_scheduler.executor import RunExecutor
from interloper_scheduler.services import scheduler_services

logger = logging.getLogger(__name__)


def register_scheduler(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``scheduler`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "scheduler",
        help="Run the scheduler services",
        description="Run the scheduler services: cron (with hooks and credential renewal), the queue worker "
        "and the reaper. Flags left unset fall back to the cron, worker and reaper settings; at least one "
        "service must be enabled.",
    )
    for name, help_text in (
        ("cron", "Run the cron controller, with the hook and renewal controllers"),
        ("worker", "Run the queue worker"),
        ("reaper", "Run the reaper (fails silent and overdue runs; singleton)"),
    ):
        parser.add_argument(f"--{name}", action=argparse.BooleanOptionalAction, default=None, help=help_text)
    parser.add_argument(
        "--no-create-tables",
        action="store_true",
        default=False,
        help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
    )
    parser.set_defaults(handler=_cmd_scheduler)


def register_launch(subparsers: argparse._SubParsersAction) -> None:
    ...  # moved verbatim from core's commands/launch.py register (no `requires`)


def _cmd_scheduler(args: argparse.Namespace) -> None:
    """Run the enabled scheduler services until a signal or a service's death.

    Args:
        args: Parsed CLI arguments: the ``cron``/``worker``/``reaper`` toggles and ``no_create_tables``.

    Raises:
        SystemExit: When no service is enabled.
    """
    settings = AppSettings.get()
    cron = settings.cron.enabled if args.cron is None else args.cron
    worker = settings.worker.enabled if args.worker is None else args.worker
    reaper = settings.reaper.enabled if args.reaper is None else args.reaper
    if not (cron or worker or reaper):
        raise SystemExit("Error: No scheduler service enabled. Enable at least one of: --cron, --worker, --reaper.")
    configure_console_logging(
        logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=settings.log.format
    )
    bootstrap(
        settings.postgres.dsn,
        statement_timeout=settings.postgres.statement_timeout,
        create_tables=not args.no_create_tables,
    )
    catalog = Catalog.from_settings()
    store = Store.from_settings(catalog=catalog)
    services = scheduler_services(settings, store=store, catalog=catalog, cron=cron, worker=worker, reaper=reaper)
    Supervisor(services).run()


def _cmd_launch(args: argparse.Namespace) -> None:
    """Execute one persisted run; a failure before the executor takes over marks the run failed.

    Args:
        args: Parsed CLI arguments, carrying ``run_id``.

    Raises:
        SystemExit: On any failure, or when the run did not succeed.
    """
    settings = AppSettings.get()
    configure_console_logging(
        logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=settings.log.format
    )
    try:
        catalog = Catalog.from_settings()
        logger.info("Catalog: %s", catalog.to_paths())
        store = Store.from_settings(catalog=catalog)
        runner = Runner.from_settings(settings.runner)
        executor = RunExecutor(store=store, runner=runner, reaper=settings.reaper, on_lost=_exit_process_lost)
        success = executor.execute(args.run_id)
    except Exception as error:
        logger.exception("Launch failed for run %s", args.run_id)
        _mark_failed(args.run_id)
        raise SystemExit(1) from error
    if not success:
        raise SystemExit(1)


def _mark_failed(run_id: UUID) -> None:
    """Record a run that could not be launched as failed, through a catalog-free store.

    Args:
        run_id: The run.
    """
    try:
        Store.from_settings().runs.complete(run_id, success=False)
    except Exception:
        logger.exception("Failed to mark run %s as failed in DB", run_id)


def _exit_process_lost() -> None:
    ...  # moved verbatim from core's commands/launch.py
```

(`os` and `sys` are used by `_exit_process_lost`. `configure_console_logging` moves here as is;
6b replaces it.)

In `packages/interloper-scheduler/pyproject.toml`:

```toml
[project.entry-points."interloper.cli"]
launch = "interloper_scheduler.cli:register_launch"
scheduler = "interloper_scheduler.cli:register_scheduler"
```

Remove `launch` from core's group, delete core's `commands/launch.py` and its test, and run
`UV_FROZEN=1 uv sync --all-packages --all-extras`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-scheduler packages/interloper-core/tests/cli -q -p no:cacheprovider`
Expected: PASS.

---

### Task 7: `api` lives in `interloper-api`

**Files:**
- Create: `packages/interloper-api/src/interloper_api/server.py`, `packages/interloper-api/src/interloper_api/cli.py`
- Modify: `packages/interloper-api/pyproject.toml`, `packages/interloper-api/src/interloper_api/__init__.py`
- Test: `packages/interloper-api/tests/test_server.py`, `packages/interloper-api/tests/test_cli.py`

**Interfaces:**
- Consumes: Task 1's `Supervisor`; Task 4's `bootstrap`.
- Produces:
  - `interloper_api.ApiServer`, with `app: FastAPI`, `start() -> None` and `stop() -> None`;
  - `ApiServer.from_settings(settings: AppSettings, *, store: Store, catalog: Catalog, port: int,
    cors_origins: list[str] | None = None) -> ApiServer`;
  - the `api` command at `interloper_api.cli:register`.

- [ ] **Step 1: Write the failing tests**

`tests/test_server.py`:

```python
"""Tests for ``interloper_api.server``."""

from interloper.catalog import Catalog
from interloper.settings import AppSettings, ServerSettings
from interloper_api import ApiServer


def test_binds_the_settings_host_and_the_given_port():
    settings = AppSettings(server=ServerSettings(host="127.0.0.1", port=3000))

    server = ApiServer.from_settings(settings, store=None, catalog=Catalog(), port=3101)  # ty: ignore[invalid-argument-type]

    assert (server._server.config.host, server._server.config.port) == ("127.0.0.1", 3101)


def test_stop_asks_uvicorn_to_exit():
    server = ApiServer.from_settings(AppSettings(), store=None, catalog=Catalog(), port=3101)  # ty: ignore[invalid-argument-type]

    server.stop()

    assert server._server.should_exit is True
```

`create_app(store=None, ...)` must accept `None`, as its signature already does; if the factory
dereferences the store at construction, pass a `SimpleNamespace` stub instead and note it.

`tests/test_cli.py`:

```python
"""Tests for ``interloper_api.cli``."""

import argparse
from typing import Any

import pytest

from interloper.settings import AppSettings
from interloper_api import cli


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    cli.register(parser.add_subparsers(dest="command"))
    return parser.parse_args(argv)


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(AppSettings, "_active", AppSettings())
    monkeypatch.setattr(cli, "bootstrap", lambda dsn, **kwargs: recorded.setdefault("bootstrap", kwargs))
    monkeypatch.setattr(cli.Catalog, "from_settings", classmethod(lambda cls: cli.Catalog()))
    monkeypatch.setattr(cli.Store, "from_settings", classmethod(lambda cls, catalog=None: object()))
    monkeypatch.setattr(cli.ApiServer, "from_settings",
                        classmethod(lambda cls, settings, **kwargs: recorded.setdefault("server", (settings, kwargs))))
    monkeypatch.setattr(cli.Supervisor, "run", lambda self: recorded.setdefault("ran", list(self.services)))
    return recorded


def test_serves_one_api_on_the_port_flag(wiring: dict[str, Any]) -> None:
    cli._cmd_api(_parse("api", "--port", "3101", "--host", "127.0.0.1"))

    settings, kwargs = wiring["server"]
    assert kwargs["port"] == 3101
    assert settings.server.host == "127.0.0.1"
    assert wiring["ran"] == ["api"]


def test_no_create_tables_reaches_the_bootstrap(wiring: dict[str, Any]) -> None:
    cli._cmd_api(_parse("api", "--no-create-tables"))

    assert wiring["bootstrap"]["create_tables"] is False
```

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-api/tests/test_server.py packages/interloper-api/tests/test_cli.py -q -p no:cacheprovider`
Expected: FAIL (modules missing).

- [ ] **Step 3: Implement**

`server.py`, from today's `Services._build_api` minus the SPA (which interloper-app adds):

```python
"""The API as a supervised service: a uvicorn server over the FastAPI app."""

from __future__ import annotations

from typing import TYPE_CHECKING

import uvicorn

from interloper.telemetry import instrument_fastapi
from interloper_api.app import create_app

if TYPE_CHECKING:
    from fastapi import FastAPI

    from interloper.catalog import Catalog
    from interloper.settings import AppSettings
    from interloper_db import Store


class ApiServer:
    """The HTTP API, served by uvicorn until stopped.

    Attributes:
        app: The FastAPI application; a composing process may add middleware to it before ``start``.
    """

    def __init__(self, app: FastAPI, server: uvicorn.Server) -> None:
        """Hold the application and its server.

        Args:
            app: The FastAPI application.
            server: The uvicorn server bound to it.
        """
        self.app = app
        self._server = server

    @classmethod
    def from_settings(
        cls,
        settings: AppSettings,
        *,
        store: Store,
        catalog: Catalog,
        port: int,
        cors_origins: list[str] | None = None,
    ) -> ApiServer:
        """Build the API and its server from settings.

        Args:
            settings: The active settings; ``server.host`` is the bind host.
            store: The store the routes read and write through.
            catalog: The catalog the routes describe.
            port: The port to bind.
            cors_origins: Origins allowed cross-origin requests; ``None`` allows none (dev mode passes the Nuxt origin).

        Returns:
            The server, not yet started.
        """
        app = create_app(store=store, catalog=catalog, settings=settings, cors_origins=cors_origins)
        instrument_fastapi(app)
        config = uvicorn.Config(
            app, host=settings.server.host, port=port, log_level="info", log_config=None, ws="wsproto"
        )
        return cls(app, uvicorn.Server(config))

    def start(self) -> None:
        """Serve until stopped."""
        self._server.run()

    def stop(self) -> None:
        """Ask the server to finish its requests and return."""
        self._server.should_exit = True
```

(Check that `instrument_fastapi` is exported from `interloper.telemetry`; it is defined in
`telemetry/setup.py` and imported from there by today's `runtime.py`. Import it from wherever
`runtime.py` does.)

`cli.py`:

```python
"""``interloper api``: serve the HTTP API, the api role's command."""

from __future__ import annotations

import argparse
import logging

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.settings import AppSettings
from interloper.utils import Supervisor
from interloper_api.server import ApiServer
from interloper_db import Store
from interloper_db.provision import bootstrap


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``api`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "api",
        help="Serve the HTTP API",
        description="Serve the HTTP API. The frontend is served separately (nginx in containers, "
        "`interloper app` locally).",
    )
    parser.add_argument("--host", default=None, help="Bind host (default: server.host, 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None, help="Bind port (default: server.port, 3000)")
    parser.add_argument(
        "--no-create-tables",
        action="store_true",
        default=False,
        help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
    )
    parser.set_defaults(handler=_cmd_api)


def _cmd_api(args: argparse.Namespace) -> None:
    """Serve the API until a signal or its death.

    Args:
        args: Parsed CLI arguments: ``host``, ``port`` and ``no_create_tables``.
    """
    settings = AppSettings.get()
    if args.host is not None:
        settings = settings.model_copy(update={"server": settings.server.model_copy(update={"host": args.host})})
        AppSettings.activate(settings)
    configure_console_logging(
        logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=settings.log.format
    )
    bootstrap(
        settings.postgres.dsn,
        statement_timeout=settings.postgres.statement_timeout,
        create_tables=not args.no_create_tables,
    )
    catalog = Catalog.from_settings()
    store = Store.from_settings(catalog=catalog)
    port = settings.server.port if args.port is None else args.port
    Supervisor({"api": ApiServer.from_settings(settings, store=store, catalog=catalog, port=port)}).run()
```

Export `ApiServer` from `interloper_api/__init__.py`. In `packages/interloper-api/pyproject.toml`:

```toml
[project.entry-points."interloper.cli"]
api = "interloper_api.cli:register"
```

Run `UV_FROZEN=1 uv sync --all-packages --all-extras`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-api -q -p no:cacheprovider > /tmp/t7.log 2>&1; tail -1 /tmp/t7.log`
Expected: all passed.

---

### Task 8: `app` lives in `interloper-app`, composing the roles

**Files:**
- Create: `packages/interloper-app/src/interloper_app/services.py`, `packages/interloper-app/src/interloper_app/cli.py`
- Modify: `packages/interloper-app/src/interloper_app/assets.py` (add `SpaFallback`),
  `packages/interloper-app/src/interloper_app/__init__.py`, `packages/interloper-app/pyproject.toml`,
  `packages/interloper-core/pyproject.toml` (drop `app`)
- Delete: `packages/interloper-core/src/interloper/cli/runtime.py`, `.../cli/commands/app.py`,
  `packages/interloper-core/tests/cli/test_runtime.py`, `.../tests/cli/commands/test_app.py`
- Test: `packages/interloper-app/tests/test_services.py`, `test_cli.py`, `test_assets.py`

**Interfaces:**
- Consumes:
  - Task 1's `Supervisor`/`Service`;
  - Task 6's `scheduler_services`;
  - Task 7's `ApiServer`;
  - Task 4's `bootstrap`.
- Produces:
  - `interloper_app.SpaFallback(app: ASGIApp, directory: Path)`, ASGI middleware added with
    `api_server.app.add_middleware(SpaFallback, directory=static_dir())`;
  - `interloper_app.services.App.from_settings(settings, *, store, catalog, api, cron, worker, reaper,
    dev, port) -> App`, with `services: dict[str, Service]` and `run() -> None`;
  - `DevServer`, `ParentWatchdog`;
  - the `app` command at `interloper_app.cli:register`.

- [ ] **Step 1: Write the failing tests**

Port the 52 tests of `tests/cli/test_runtime.py` and the tests of `tests/cli/commands/test_app.py`
to the new owners. Each test keeps its behaviour; only its subject moves:

| Test subject today | New home | New subject |
|---|---|---|
| `apply_cli_overrides` | `test_cli.py` | `_cmd_app`'s overrides (assert on the settings `App.from_settings` receives) |
| `resolve_api_port`, `_find_free_port` | `test_services.py` | `App._api_port(...)` |
| `Services._build_*`, `_start_threads` (which services exist) | `test_services.py` | `App.from_settings(...).services` keys |
| `_shutdown`, signals, joins | already covered by core's `tests/utils/test_supervisor.py` | delete |
| `_run_nuxt_dev`, `_signal_nuxt`, `_kill_process_group`, `_port_in_use`, `_prepare_nuxt` | `test_services.py` | `DevServer` |
| `_watch_parent` | `test_services.py` | `ParentWatchdog` |
| `_mount_spa` | `test_assets.py` | `SpaFallback` |
| `_cmd_app` wiring (`test_app.py`) | `test_cli.py` | `_cmd_app` |

New tests:

```python
# test_cli.py
def test_dev_without_the_api_is_refused(wiring: dict[str, Any]) -> None:
    """The dev server only proxies to an API, so ``--dev --no-api`` is an error."""
    with pytest.raises(SystemExit, match="--dev"):
        cli._cmd_app(_parse("app", "--dev", "--no-api", "--cron"))


# test_assets.py
async def test_the_api_prefix_matches_the_segment_only(tmp_path: Path) -> None:
    """``/api`` and ``/api/...`` reach the API; ``/apifoo`` is an SPA route served index.html."""
    (tmp_path / "index.html").write_text("<html>spa</html>")
    reached: list[str] = []
    sent: list[dict[str, Any]] = []

    async def downstream(scope: dict[str, Any], receive: Any, send: Any) -> None:
        reached.append(scope["path"])

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b""}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    middleware = SpaFallback(downstream, directory=tmp_path)
    for path in ("/api", "/api/health", "/apifoo"):
        scope = {"type": "http", "path": path, "method": "GET", "headers": [], "query_string": b""}
        await middleware(scope, receive, send)

    assert reached == ["/api", "/api/health"]
    assert any(message.get("status") == 200 for message in sent)
```

(`SpaFallback` from `interloper_app`; `Path` from `pathlib`; `Any` from `typing`. The root
`pyproject.toml` sets `asyncio_mode = "auto"`.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-app/tests -q -p no:cacheprovider`
Expected: FAIL (modules missing).

- [ ] **Step 3: Implement**

`assets.py` gains `SpaFallback`, the body of today's nested `SPAFallbackMiddleware`, as a
module-level class:
- `__init__(self, app: ASGIApp, directory: Path)` builds `StaticFiles(directory=directory)` and the
  `index.html` path.
- `__call__` delegates to the wrapped app when
  `scope["type"] != "http" or scope["path"] == "/api" or scope["path"].startswith("/api/")`.

Export it from `__init__.py`.

`services.py` holds three classes:
- `DevServer` (service):
  - `start()` runs today's `_run_nuxt_dev` body (Popen `pnpm dev` in its own session, waits);
  - `stop()` sends SIGTERM to the process group;
  - the occupied-port check (`_port_in_use`) is a `classmethod` `DevServer.check_port(port)` raising
    `SystemExit` with today's message; its loop variables are named `family, address` and its
    socket `probe`, not `addr`/`sock`;
  - the `atexit` SIGKILL registration happens in `__init__`.

  When `pnpm dev` exits on its own, `start` returns, which the supervisor treats as a death: the
  same outcome as today's `_shutdown("nuxt dev server died")`.
- `ParentWatchdog` (service): `start()` polls `os.getppid()` once a second until stopped or until
  the parent changed, then returns, which is a death. `stop()` sets its event.
- `App`:
  - **`from_settings(settings, *, store, catalog, api, cron, worker, reaper, dev, port)`**:
    - raises `SystemExit("Error: --dev runs the Nuxt dev server against the API; it cannot be used with --no-api.")`
      when `dev and not api`;
    - raises `SystemExit("Error: No services enabled. Enable at least one of: --api, --cron, --worker, --reaper.")`
      when nothing is enabled;
    - otherwise builds, in order:
      - `"api"`, an `ApiServer` on `self._api_port(settings, dev=dev, port=port)`, with
        `cors_origins=[f"http://localhost:{settings.server.port}"]` in dev and the `SpaFallback`
        over `static_dir()` outside dev. A missing build is a logged warning, as today;
      - the `scheduler_services(...)` entries;
      - in dev, `"nuxt-dev"` (`DevServer`) and `"parent-watchdog"` (`ParentWatchdog`).
  - **`_api_port`** is today's `resolve_api_port` and `_find_free_port`, as a `staticmethod`:
    - in dev with no explicit port, a free port;
    - otherwise the explicit port or `server.port`.
  - **`run()`** checks the dev port with `DevServer.check_port(settings.server.port)` in dev, then
    calls `Supervisor(self.services).run()`.

`cli.py` holds `register` (moved verbatim from core's `commands/app.py`: same flags, help and
description, with the docstring corrected to name the reaper and drop "and/or") and `_cmd_app`:

```python
def _cmd_app(args: argparse.Namespace) -> None:
    """Run the application services together: the API, the scheduler's, and in dev the Nuxt dev server.

    Args:
        args: Parsed CLI arguments: the service toggles, ``host``, ``port``, ``dev`` and ``no_create_tables``.
    """
    settings = _with_overrides(args, AppSettings.get())
    AppSettings.activate(settings)
    configure_console_logging(
        logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=settings.log.format
    )
    bootstrap(
        settings.postgres.dsn,
        statement_timeout=settings.postgres.statement_timeout,
        create_tables=not args.no_create_tables,
    )
    catalog = Catalog.from_settings()
    store = Store.from_settings(catalog=catalog)
    App.from_settings(
        settings,
        store=store,
        catalog=catalog,
        api=settings.server.enabled,
        cron=settings.cron.enabled,
        worker=settings.worker.enabled,
        reaper=settings.reaper.enabled,
        dev=args.dev,
        port=args.port,
    ).run()
```

`_with_overrides(args, settings) -> AppSettings` is today's `apply_cli_overrides`, kept private to
this command, since it is flag handling.

In `packages/interloper-app/pyproject.toml`:

```toml
[project.entry-points."interloper.cli"]
app = "interloper_app.cli:register"
```

Remove `app` from core's group, so core's group holds only `run`. Delete `runtime.py`,
`commands/app.py` and their tests, then run `UV_FROZEN=1 uv sync --all-packages --all-extras`.

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-app packages/interloper-core/tests/cli -q -p no:cacheprovider`
Expected: PASS; `grep -rn "interloper_api\|interloper_db\|interloper_scheduler\|interloper_app" packages/interloper-core/src`
prints nothing.

---

### Task 9: `run` cleanup

**Files:**
- Modify: `packages/interloper-core/src/interloper/cli/commands/run.py`, `packages/interloper-core/src/interloper/dag/base.py`
- Test: `packages/interloper-core/tests/cli/commands/test_run.py`, `packages/interloper-core/tests/dag/test_base.py`

**Interfaces:**
- Produces: `DAG.from_paths(paths: list[str]) -> DAG`, raising `DAGError` for an import failure, a
  non-class, or a class that is not a `Workload`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/dag/test_base.py
class TestFromPaths:
    """A DAG built from dotted import paths."""

    def test_builds_from_workload_classes(self):
        dag = DAG.from_paths([f"{__name__}.FakeAsset"])

        assert [operation.key for operation in dag.operations] == ["fake_asset"]

    def test_an_unimportable_path_is_a_dag_error(self):
        with pytest.raises(DAGError, match="failed to import 'nowhere.Missing'"):
            DAG.from_paths(["nowhere.Missing"])

    def test_a_non_workload_is_a_dag_error(self):
        with pytest.raises(DAGError, match="not a Source or Asset"):
            DAG.from_paths(["interloper.MemoryDestination"])


# tests/cli/commands/test_run.py
def test_file_and_format_are_exclusive(capsys: pytest.CaptureFixture[str]) -> None:
    """``-f`` with ``--format`` is refused by the parser instead of ignoring ``--format``."""
    parser = argparse.ArgumentParser()
    run_command.register(parser.add_subparsers(dest="command"))

    with pytest.raises(SystemExit):
        parser.parse_args(["run", "-f", "spec.yaml", "--format", "inline"])

    assert "not allowed with argument" in capsys.readouterr().err
```

(Use the key `FakeAsset` actually derives; read the fixture's `key`.)

- [ ] **Step 2: Run them to verify they fail**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/dag/test_base.py packages/interloper-core/tests/cli/commands/test_run.py -q -p no:cacheprovider -k "FromPaths or exclusive"`
Expected: FAIL.

- [ ] **Step 3: Implement**

- **`DAG.from_paths`** goes in `dag/base.py`, next to `from_spec_file`:

  ```python
      @classmethod
      def from_paths(cls, paths: list[str]) -> DAG:
          """Build a DAG from dotted import paths, one Source or Asset class each.

          Args:
              paths: Dotted (or composite ``module:Class``) import paths.

          Returns:
              The DAG over one default instance of each class.

          Raises:
              DAGError: If a path fails to import, or names something other than a Source or Asset class.
          """
          workloads: list[type[Workload]] = []
          for path in paths:
              try:
                  imported = import_from_path(path)
              except (ImportError, AttributeError) as error:
                  raise DAGError(f"failed to import '{path}': {error}") from error
              if not (isinstance(imported, type) and issubclass(imported, Workload)):
                  raise DAGError(f"'{path}' is not a Source or Asset class")
              workloads.append(imported)
          return cls(*workloads)
  ```

  with `from interloper.utils import import_from_path`.
- **`run.py`:**
  - put `--format` and `-f/--file` in one `run_parser.add_mutually_exclusive_group()`;
  - the paths branch calls `DAG.from_paths(args.target)`, with `DAGError` turned into
    `SystemExit(f"Error: {error}")`;
  - delete `_dag_from_paths`;
  - drop `_print_plan`'s `name` parameter and its `Run:` line;
  - import `DAG` once at the top of the handler, deleting the `_DAG` alias;
  - rename `exc` to `error`;
  - remove the trailing "Destination" from the docstrings;
  - the "manifest" wording becomes "spec".

- [ ] **Step 4: Run the tests**

Run: `UV_FROZEN=1 uv run pytest packages/interloper-core/tests/cli packages/interloper-core/tests/dag -q -p no:cacheprovider`
Expected: PASS.

---

### Task 10: Images, chart, docs, upgrade row, verification

**Files:**
- Modify:
  - **Images and chart:** `dockerfile` (the `api`, `api-slim`, `scheduler` and `scheduler-slim` CMDs),
    `chart/interloper/templates/api-deployment.yaml`, `chart/interloper/templates/scheduler-deployment.yaml`.
  - **Docs:** `docs/guide/cli.md`, `docs/ui/running.md`.
  - **Plugin skills:** `plugins/interloper/skills/interloper-deploy/SKILL.md`,
    `plugins/interloper/skills/interloper-triage/SKILL.md`,
    `plugins/interloper/skills/interloper-upgrade/SKILL.md`.
  - **Repo docs:** `README.md`, `AGENTS.md` (the layout's `interloper-core` line: "framework: assets,
    sources, DAG, runners, IO, partitioning, the `run` command").

- [ ] **Step 1: Images and chart**

| File | Before | After |
|---|---|---|
| dockerfile `api` and `api-slim` CMD | `["interloper", "app", "--api", "--no-cron", "--no-worker", "--no-reaper", "--no-create-tables"]` | `["interloper", "api", "--no-create-tables"]` |
| dockerfile `scheduler` and `scheduler-slim` CMD | `["interloper", "app", "--no-api", "--cron", "--worker", "--reaper", "--no-create-tables"]` | `["interloper", "scheduler", "--no-create-tables"]` |
| `api-deployment.yaml` args | `app`, `--api`, `--no-cron`, `--no-worker`, `--no-reaper`, `--no-create-tables` | `api`, `--no-create-tables` |
| `scheduler-deployment.yaml` args | `app`, `--no-api`, `--cron`, `--worker`, `--reaper`, `--no-create-tables` | `scheduler`, `--no-create-tables` |

Run: `helm template chart/interloper | grep -A3 'command: \["interloper"\]'`
Expected: `api` and `scheduler` args.

- [ ] **Step 2: Docs and skills**

- **`docs/guide/cli.md`:**
  - the command list names `api`, `scheduler` and `app` with one line each (role command,
    role command, all-in-one for local and single-process);
  - each command is available when its package is installed;
  - the requirements prose goes.
- **`docs/ui/running.md`:** the image commands match Step 1.
- **The deploy and triage skills:** replace `interloper app --api ...`/`--no-api ...` with the role
  commands where they describe the images.
- **Upgrade skill:** a row above the phase 5 row:

  | unreleased (core phase 6a) | `from interloper.errors import NotFoundError` (and `ConflictError`, `InUseError`, `QuotaExceededError`, `HydrationError`, `ComponentDriftError`); `interloper app --api --no-cron ...` / `--no-api --cron ...` in a custom image or chart override; a command refused for a missing package | `from interloper_db import NotFoundError` (same for the others); `interloper api` / `interloper scheduler` (move chart and images together: the chart's image tag defaults to its app version); the command is absent until its package is installed |

- [ ] **Step 3: Full check, docs build, catalog**

Run: `UV_FROZEN=1 make check-python > /tmp/check.log 2>&1; tail -3 /tmp/check.log`
Expected: all green.
Run: `UV_FROZEN=1 uv run zensical build --strict > /tmp/docs.log 2>&1; echo $?`
Expected: `0`; the CLI reference lists `api`, `app`, `db`, `launch`, `run` and `scheduler`; the
errors reference shows both modules.
Catalog: dump `il.Catalog.discover().dump()` (sorted JSON) here and in the phase 5 worktree
(`.claude/worktrees/refactor/core-surface`), then `diff -q`.
Expected: identical.

- [ ] **Step 4: Live verification**

- **`make dev-up`** on port 3100 (`INTERLOPER_SERVER_PORT=3100`, background): the instance comes up
  with every service, the Nuxt port is read from the log, `/api/health` answers 200, and stopping
  the task leaves the port free.
- **`interloper api --port 3102 --no-create-tables`** from `dev/`, with the dev `INTERLOPER_*`
  variables, in the background: `/api/health` answers 200. Mint a session, then trigger a one-day
  run of the demo source through the API.
- **`interloper scheduler --no-create-tables`**, same environment, in the background: its worker
  picks the run up and it completes. SIGTERM each process: both exit 0 and log `Shutdown complete.`
- **`interloper db upgrade`** against the dev database: `Done.`, and `alembic_version` is unchanged.
- **Images:** `make docker-build-api docker-build-scheduler`, then
  `docker run --rm interloper-api:<tag> interloper api --help` and the scheduler equivalent print
  their help.
- **Cleanup:** delete the minted session row.

- [ ] **Step 5: Checkpoint (no commit)**

The one commit happens after the final whole-branch review, per the standing rule.
