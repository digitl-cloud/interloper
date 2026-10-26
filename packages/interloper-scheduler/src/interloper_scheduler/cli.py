"""The scheduler's commands: ``interloper scheduler`` runs its services, ``interloper launch`` one run."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from uuid import UUID

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.errors import format_exception
from interloper.runner import Runner
from interloper.settings import AppSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap

from interloper_scheduler.executor import RunExecutor
from interloper_scheduler.services import scheduler_services

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"


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
    parser.add_argument(
        "--cron",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the cron controller, with the hook and renewal controllers",
    )
    parser.add_argument("--worker", action=argparse.BooleanOptionalAction, default=None, help="Run the queue worker")
    parser.add_argument(
        "--reaper",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the reaper (fails silent and overdue runs; singleton)",
    )
    parser.add_argument(
        "--no-create-tables",
        action="store_true",
        default=False,
        help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
    )
    parser.set_defaults(handler=_cmd_scheduler)


def register_launch(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``launch`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "launch",
        help="Execute a single run by ID",
        description="Execute one persisted run. On any failure before the executor takes over, the run is "
        "marked failed.",
    )
    parser.add_argument("run_id", type=UUID, help="The UUID of the run to execute")
    parser.set_defaults(handler=_cmd_launch)


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
    configure_console_logging(logging.INFO, _LOG_FORMAT, log_format=settings.log.format)
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

    The catalog, the store and the runner are all built inside the guard, so a
    bad setting fails the run instead of leaving it ``dispatched``.

    Args:
        args: Parsed CLI arguments, carrying ``run_id``.

    Raises:
        SystemExit: On any failure, or when the run did not succeed.
    """
    settings = AppSettings.get()
    configure_console_logging(logging.INFO, _LOG_FORMAT, log_format=settings.log.format)
    try:
        catalog = Catalog.from_settings()
        logger.info("Catalog: %s", catalog.to_paths())
        store = Store.from_settings(catalog=catalog)
        runner = Runner.from_settings(settings.runner)
        executor = RunExecutor(store=store, runner=runner, reaper=settings.reaper, on_lost=_exit_process_lost)
        success = executor.execute(args.run_id)
    except Exception as error:
        logger.exception("Launch failed for run %s", args.run_id)
        _mark_failed(args.run_id, error)
        raise SystemExit(1) from error
    if not success:
        raise SystemExit(1)


def _mark_failed(run_id: UUID, error: BaseException) -> None:
    """Record a run that could not be launched as failed, through a store over an empty catalog.

    The configured catalog may be the very thing that failed to load, and
    marking a run touches no component definition.

    Args:
        run_id: The run.
        error: Why the launch failed, recorded as the run's reason.
    """
    try:
        Store.from_settings(catalog=Catalog(components={})).runs.fail(run_id, format_exception(error))
    except Exception:
        logger.exception("Failed to mark run %s as failed in DB", run_id)


def _exit_process_lost() -> None:
    """Exit this run's process at once: its run was ended elsewhere, or can no longer prove it is alive.

    The process exists for this one run, and an operation blocked in sync code
    cannot be interrupted any other way. Exiting without unwinding is what a
    pod eviction does anyway, and a retry rewrites whatever partition was
    being written.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)
