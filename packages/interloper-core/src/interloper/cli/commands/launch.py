"""``interloper launch`` — execute a single run by ID."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from uuid import UUID

logger = logging.getLogger(__name__)


def register(
    subparsers: argparse._SubParsersAction,
) -> None:
    """Register the ``launch`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    launch_parser = subparsers.add_parser(
        "launch",
        help="Execute a single run by ID",
        description="Execute one persisted run. On any failure before the executor takes over, the run is "
        "marked failed.",
    )
    launch_parser.add_argument(
        "run_id",
        type=UUID,
        help="The UUID of the run to execute",
    )
    launch_parser.set_defaults(
        handler=_cmd_launch,
        requires=["interloper_db", "interloper_scheduler"],
    )


def _cmd_launch(args: argparse.Namespace) -> None:
    """Execute a single run.

    If anything fails before the executor takes over (e.g. missing
    package, bad settings), the run is marked as failed in the DB so
    it doesn't stay stuck in ``dispatched`` status.

    Args:
        args: Parsed CLI arguments, carrying ``run_id`` (the UUID of the run to execute).

    Raises:
        SystemExit: On any failure.
    """
    from interloper.catalog import Catalog
    from interloper.runner import Runner
    from interloper.settings import AppSettings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    settings = AppSettings.get()

    from interloper_db import Store

    catalog = Catalog.from_settings()
    logger.info(f"Catalog: {catalog.to_paths()}")

    store = Store.from_settings(catalog=catalog)

    try:
        from interloper_scheduler import RunExecutor

        runner = Runner.from_settings(settings.runner)
        executor = RunExecutor(store=store, runner=runner, reaper=settings.reaper, on_lost=_exit_process_lost)
        success = executor.execute(args.run_id)
    except Exception as e:
        logger.exception("Launch failed for run %s", args.run_id)
        try:
            store.runs.complete(args.run_id, success=False)
        except Exception:
            logger.exception("Failed to mark run %s as failed in DB", args.run_id)
        raise SystemExit(1) from e

    if not success:
        raise SystemExit(1)


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
