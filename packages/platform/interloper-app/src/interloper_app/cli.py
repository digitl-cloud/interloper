"""``interloper app``: the API, the scheduler's services and the frontend together, in one process.

The command for single-process deployments; containers run each role
through its own command (``interloper api``, ``interloper scheduler``), and
local work runs through ``interloper dev``. ``SIGINT``, ``SIGTERM`` and
``SIGHUP`` stop it gracefully, and so does the death of any service.
"""

from __future__ import annotations

import argparse
import logging
import time

from interloper.catalog import Catalog
from interloper.cli import Command
from interloper.cli.console import configure_console_logging
from interloper.settings import LogSettings, ServerSettings
from interloper.utils import ReadyLine

logger = logging.getLogger(__name__)


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
            "-v", "--verbose", action="store_true", help="Print debug lines, every request, and libraries' info lines"
        )

    def run(self, args: argparse.Namespace) -> None:
        """Run the application services together until a signal or a service's death.

        Args:
            args: The parsed command line: the service toggles, ``host``, ``port`` and ``verbose``.
        """
        from interloper_api.settings import ApiSettings
        from interloper_db import Store
        from interloper_db.provision import connect
        from interloper_db.settings import PostgresSettings
        from interloper_scheduler.settings import SchedulerSettings

        from interloper_app.services import App

        started = time.time()
        self._activate_flags(args)
        configure_console_logging(logging.DEBUG if args.verbose else logging.INFO, log_format=LogSettings.get().format)
        postgres = PostgresSettings.get()
        connect(postgres.dsn, statement_timeout=postgres.statement_timeout)
        api_settings = ApiSettings.get()
        scheduler_settings = SchedulerSettings.get()
        catalog = Catalog.from_settings()
        store = Store.from_settings(
            catalog=catalog,
            session_expiry=api_settings.auth.session_expiry,
            access_token_expiry=api_settings.auth.access_token_expiry,
            refresh_token_expiry=api_settings.auth.refresh_token_expiry,
        )
        app = App.from_settings(
            api_settings,
            scheduler_settings,
            store=store,
            catalog=catalog,
            api=api_settings.server.enabled,
            cron=scheduler_settings.cron.enabled,
            worker=scheduler_settings.worker.enabled,
            reaper=scheduler_settings.reaper.enabled,
        )
        url = api_settings.server.public_url if api_settings.server.enabled else None
        app.run(ReadyLine(logger, started, list(app.services), len(catalog.components), url))

    def _activate_flags(self, args: argparse.Namespace) -> None:
        """Activate the sections the command's flags override, with the flags applied; an unset flag keeps its setting.

        Args:
            args: The parsed command line: ``host``, ``port`` and the ``api``/``cron``/``worker``/``reaper`` toggles.
        """
        from interloper_scheduler.settings import CronSettings, ReaperSettings, WorkerSettings

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
