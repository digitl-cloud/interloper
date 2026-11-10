"""The scheduler's command: ``interloper scheduler`` runs its services."""

from __future__ import annotations

import argparse
import logging

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.settings import AppSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap

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
