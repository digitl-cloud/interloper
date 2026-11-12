"""The scheduler's command: ``interloper scheduler`` runs its services."""

from __future__ import annotations

import argparse
import logging

from interloper.catalog import Catalog
from interloper.cli import Command
from interloper.cli.console import configure_console_logging
from interloper.settings import LogSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap
from interloper_db.settings import PostgresSettings

from interloper_scheduler.services import scheduler_services
from interloper_scheduler.settings import SchedulerSettings

logger = logging.getLogger(__name__)

_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"


class SchedulerCommand(Command):
    """Run the scheduler services.

    The services are cron (with hooks and credential renewal), the queue worker and the reaper. Flags left unset
    fall back to the cron, worker and reaper settings; at least one service must be enabled.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare which services to run and whether to bootstrap the tables.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument(
            "--cron",
            action=argparse.BooleanOptionalAction,
            default=None,
            help="Run the cron controller, with the hook and renewal controllers",
        )
        parser.add_argument(
            "--worker", action=argparse.BooleanOptionalAction, default=None, help="Run the queue worker"
        )
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

    def run(self, args: argparse.Namespace) -> None:
        """Run the enabled scheduler services until a signal or a service's death.

        Args:
            args: The parsed command line: the ``cron``/``worker``/``reaper`` toggles and ``no_create_tables``.

        Raises:
            SystemExit: When no service is enabled.
        """
        settings = SchedulerSettings.get()
        cron = settings.cron.enabled if args.cron is None else args.cron
        worker = settings.worker.enabled if args.worker is None else args.worker
        reaper = settings.reaper.enabled if args.reaper is None else args.reaper
        if not (cron or worker or reaper):
            raise SystemExit("Error: No scheduler service enabled. Enable at least one of: --cron, --worker, --reaper.")
        configure_console_logging(logging.INFO, _LOG_FORMAT, log_format=LogSettings.get().format)
        postgres = PostgresSettings.get()
        bootstrap(postgres.dsn, statement_timeout=postgres.statement_timeout, create_tables=not args.no_create_tables)
        catalog = Catalog.from_settings()
        store = Store.from_settings(catalog=catalog)
        services = scheduler_services(settings, store=store, catalog=catalog, cron=cron, worker=worker, reaper=reaper)
        Supervisor(services).run()
