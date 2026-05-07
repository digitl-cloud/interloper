"""``interloper app`` — unified service runner.

Starts any combination of the API server, cron controller, and queue
worker depending on which packages are installed and which flags are set.

Services run concurrently in threads. ``SIGINT`` / ``SIGTERM`` trigger a
graceful shutdown.

Available when ``interloper-api`` and/or ``interloper-scheduler`` are installed.
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def register(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Register the ``app`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    app_parser = subparsers.add_parser(
        "app",
        help="Run the interloper application services",
    )
    app_parser.add_argument(
        "--host",
        default=None,
        help="Server bind host (default: 0.0.0.0)",
    )
    app_parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Server bind port (default: 3000)",
    )
    app_parser.add_argument(
        "--api",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the API server",
    )
    app_parser.add_argument(
        "--cron",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the cron controller",
    )
    app_parser.add_argument(
        "--worker",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the queue worker",
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

    app_parser.set_defaults(
        func=_cmd_app,
        requires=["interloper_db"],
        requires_when={
            "api": ["interloper_api"],
            "cron": ["interloper_scheduler"],
            "worker": ["interloper_scheduler"],
            "reaper": ["interloper_scheduler"],
        },
    )


def _cmd_app(args: argparse.Namespace) -> None:
    """Start the application services."""
    from interloper.catalog import Catalog
    from interloper.cli.runtime import resolve_api_port
    from interloper.cli.services import run_services
    from interloper.settings import AppSettings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    settings = AppSettings.get()
    run_api = settings.server.enabled
    run_cron = settings.cron.enabled
    run_worker = settings.worker.enabled
    run_reaper = settings.reaper.enabled
    dev_mode = bool(getattr(args, "dev", False))
    skip_create_tables = bool(getattr(args, "no_create_tables", False))

    if not (run_api or run_cron or run_worker or run_reaper):
        print(
            "Error: No services enabled. Enable at least one of: --api, --cron, --worker, --reaper.",
            file=sys.stderr,
        )
        sys.exit(1)

    api_port = resolve_api_port(
        settings=settings,
        run_api=run_api,
        dev_mode=dev_mode,
        explicit_port_arg=getattr(args, "port", None),
    )

    from interloper_db import Store, ensure_database, init_engine

    dsn = settings.postgres.dsn
    ensure_database(dsn)
    init_engine(dsn)

    if not skip_create_tables:
        # Dev convenience: idempotent CREATE TABLE on every startup. Concurrent
        # callers are serialized by a Postgres advisory lock inside create_all.
        # Production deploys should run `interloper db init` once via a
        # pre-install hook and pass --no-create-tables here.
        from interloper_db import create_all

        create_all()

    catalog = Catalog.from_settings()
    store = Store(catalog=catalog)

    run_services(
        settings=settings,
        store=store,
        catalog=catalog,
        run_api=run_api,
        run_cron=run_cron,
        run_worker=run_worker,
        run_reaper=run_reaper,
        dev_mode=dev_mode,
        api_port=api_port,
    )
