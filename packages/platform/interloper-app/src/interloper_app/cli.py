"""``interloper app``: the API, the scheduler's services and the frontend together, in one process.

The all-in-one command for local development and single-process deployments;
containers run each role through its own command (``interloper api``,
``interloper scheduler``). ``SIGINT``, ``SIGTERM`` and ``SIGHUP`` stop it
gracefully, and so does the death of any service, the dev server and the
parent watchdog included.
"""

from __future__ import annotations

import argparse
import logging

from interloper.catalog import Catalog
from interloper.cli import Command
from interloper.cli.console import configure_console_logging
from interloper.settings import LogSettings, ServerSettings
from interloper_api.settings import ApiSettings
from interloper_db import Store
from interloper_db.provision import bootstrap
from interloper_db.settings import PostgresSettings
from interloper_scheduler.settings import CronSettings, ReaperSettings, SchedulerSettings, WorkerSettings

from interloper_app.services import App


class AppCommand(Command):
    """Run the interloper application services.

    Flags left unset fall back to the server, cron, worker and reaper settings; at least one service must be
    enabled.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare where to bind, which services to run and how.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("--host", default=None, help="Server bind host (default: 0.0.0.0)")
        parser.add_argument("--port", type=int, default=None, help="Server bind port (default: 3000)")
        parser.add_argument("--api", action=argparse.BooleanOptionalAction, default=None, help="Run the API server")
        parser.add_argument(
            "--cron", action=argparse.BooleanOptionalAction, default=None, help="Run the cron controller"
        )
        parser.add_argument(
            "--worker", action=argparse.BooleanOptionalAction, default=None, help="Run the queue worker"
        )
        parser.add_argument(
            "--reaper",
            action=argparse.BooleanOptionalAction,
            default=None,
            help="Run the reaper (timed-out run cleanup; singleton)",
        )
        parser.add_argument(
            "--dev",
            action="store_true",
            default=False,
            help="Run Nuxt dev server instead of serving built static assets",
        )
        parser.add_argument(
            "--no-create-tables",
            action="store_true",
            default=False,
            help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
        )

    def run(self, args: argparse.Namespace) -> None:
        """Run the application services together until a signal or a service's death.

        Args:
            args: The parsed command line: the service toggles, ``host``, ``port``, ``dev`` and
                ``no_create_tables``.
        """
        self._activate_flags(args)
        configure_console_logging(
            logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=LogSettings.get().format
        )
        postgres = PostgresSettings.get()
        bootstrap(postgres.dsn, statement_timeout=postgres.statement_timeout, create_tables=not args.no_create_tables)
        api_settings = ApiSettings.get()
        scheduler_settings = SchedulerSettings.get()
        catalog = Catalog.from_settings()
        store = Store.from_settings(
            catalog=catalog,
            session_expiry=api_settings.auth.session_expiry,
            access_token_expiry=api_settings.auth.access_token_expiry,
            refresh_token_expiry=api_settings.auth.refresh_token_expiry,
        )
        App.from_settings(
            api_settings,
            scheduler_settings,
            store=store,
            catalog=catalog,
            api=api_settings.server.enabled,
            cron=scheduler_settings.cron.enabled,
            worker=scheduler_settings.worker.enabled,
            reaper=scheduler_settings.reaper.enabled,
            dev=args.dev,
            port=args.port,
        ).run()

    def _activate_flags(self, args: argparse.Namespace) -> None:
        """Activate the sections the command's flags override, with the flags applied; an unset flag keeps its setting.

        Args:
            args: The parsed command line: ``host``, ``port`` and the ``api``/``cron``/``worker``/``reaper`` toggles.
        """
        server = {"host": args.host, "port": args.port, "enabled": args.api}
        ServerSettings.activate(
            ServerSettings.get().model_copy(update={name: value for name, value in server.items() if value is not None})
        )
        for section, enabled in (
            (CronSettings, args.cron),
            (WorkerSettings, args.worker),
            (ReaperSettings, args.reaper),
        ):
            if enabled is not None:
                section.activate(section.get().model_copy(update={"enabled": enabled}))
