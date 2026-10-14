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
from interloper.cli.console import configure_console_logging
from interloper.settings import AppSettings
from interloper_db import Store
from interloper_db.provision import bootstrap

from interloper_app.services import App


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``app`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    app_parser = subparsers.add_parser(
        "app",
        help="Run the interloper application services",
        description="Run the interloper application services. Flags left unset fall back to the server, cron, "
        "worker and reaper settings; at least one service must be enabled.",
    )
    app_parser.add_argument("--host", default=None, help="Server bind host (default: 0.0.0.0)")
    app_parser.add_argument("--port", type=int, default=None, help="Server bind port (default: 3000)")
    app_parser.add_argument("--api", action=argparse.BooleanOptionalAction, default=None, help="Run the API server")
    app_parser.add_argument(
        "--cron", action=argparse.BooleanOptionalAction, default=None, help="Run the cron controller"
    )
    app_parser.add_argument(
        "--worker", action=argparse.BooleanOptionalAction, default=None, help="Run the queue worker"
    )
    app_parser.add_argument(
        "--reaper",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the reaper (timed-out run cleanup; singleton)",
    )
    app_parser.add_argument(
        "--dev",
        action="store_true",
        default=False,
        help="Run Nuxt dev server instead of serving built static assets",
    )
    app_parser.add_argument(
        "--no-create-tables",
        action="store_true",
        default=False,
        help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
    )
    app_parser.set_defaults(handler=_cmd_app)


def _cmd_app(args: argparse.Namespace) -> None:
    """Run the application services together until a signal or a service's death.

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


def _with_overrides(args: argparse.Namespace, settings: AppSettings) -> AppSettings:
    """Fold the command's flags into the settings; a flag left unset keeps the setting.

    Args:
        args: Parsed CLI arguments: ``host``, ``port`` and the ``api``/``cron``/``worker``/``reaper`` toggles.
        settings: The loaded settings, left unchanged.

    Returns:
        A copy of the settings with the given flags applied.
    """
    server = settings.server
    if args.host is not None:
        server = server.model_copy(update={"host": args.host})
    if args.port is not None:
        server = server.model_copy(update={"port": args.port})
    if args.api is not None:
        server = server.model_copy(update={"enabled": args.api})
    toggles = {"cron": args.cron, "worker": args.worker, "reaper": args.reaper}
    sections = {
        name: getattr(settings, name).model_copy(update={"enabled": value})
        for name, value in toggles.items()
        if value is not None
    }
    return settings.model_copy(update={"server": server, **sections})
