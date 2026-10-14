"""``interloper api``: serve the HTTP API, the api role's command."""

from __future__ import annotations

import argparse
import logging

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.settings import AppSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap

from interloper_api.server import ApiServer


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``api`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "api",
        help="Serve the HTTP API",
        description="Serve the HTTP API. The frontend is served separately: by nginx in containers, by "
        "`interloper app` locally.",
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
