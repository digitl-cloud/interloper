"""``interloper launch`` — execute a single run by ID."""

from __future__ import annotations

import argparse
import logging
from uuid import UUID

logger = logging.getLogger(__name__)


def register(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Register the ``launch`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    launch_parser = subparsers.add_parser(
        "launch",
        help="Execute a single run by ID",
    )
    launch_parser.add_argument(
        "run_id",
        type=UUID,
        help="The UUID of the run to execute",
    )
    launch_parser.set_defaults(
        func=_cmd_launch,
        requires=["interloper_db", "interloper_scheduler"],
    )


def _cmd_launch(args: argparse.Namespace) -> None:
    """Execute a single run.

    If anything fails before the executor takes over (e.g. missing
    package, bad settings), the run is marked as failed in the DB so
    it doesn't stay stuck in ``dispatched`` status.

    Raises:
        SystemExit: On any failure.
    """
    from interloper.catalog import Catalog
    from interloper.runner import build_runner
    from interloper.settings import AppSettings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    settings = AppSettings.get()

    from interloper_db import Store, init_engine

    init_engine(settings.postgres.dsn)
    catalog = Catalog.from_settings()
    logger.info(f"Catalog: {catalog.to_paths()}")

    store = Store(catalog=catalog)

    try:
        from interloper_scheduler import RunExecutor

        runner_cls, runner_kwargs = build_runner(settings.runner.type, settings.runner.config)
        executor = RunExecutor(store=store, runner_type=runner_cls, runner_kwargs=runner_kwargs)
        success = executor.execute(args.run_id)
    except Exception as e:
        logger.exception("Launch failed for run %s", args.run_id)
        try:
            store.complete_run(args.run_id, success=False)
        except Exception:
            logger.exception("Failed to mark run %s as failed in DB", args.run_id)
        raise SystemExit(1) from e

    if not success:
        raise SystemExit(1)
