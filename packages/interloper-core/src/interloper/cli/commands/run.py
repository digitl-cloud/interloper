"""``interloper run`` — execute a DAG directly using the configured runner.

Framework-level execution with no database or persistence layer.  Used
both for direct user invocation and as the entry point that
:class:`interloper_docker.DockerRunner` calls inside spawned containers.

Input formats
-------------

* ``--format inline <json>`` — a serialized :class:`DAGSpec` as JSON.
  This is the mode used by the ``DockerRunner`` to pass a mini-DAG to a
  child container.
* ``--format paths <path>...`` — one or more dotted import paths that
  resolve to Source, Asset, or Destination classes.  The resolved
  classes are instantiated and passed to ``DAG(*items)``.

The runner used is always the one configured in ``AppSettings.runner``
(top-level ``runner`` key in ``interloper.yaml`` or
``INTERLOPER_RUNNER_*`` environment variables).
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow

logger = logging.getLogger(__name__)


def register(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Register the ``run`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    run_parser = subparsers.add_parser(
        "run",
        help="Execute a DAG directly with the configured runner (no persistence)",
    )
    run_parser.add_argument(
        "--format",
        choices=["inline", "paths"],
        default="paths",
        help="Input format: 'inline' for a DAGSpec JSON string, 'paths' for dotted import paths",
    )
    run_parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier, forwarded as metadata to the runner",
    )
    run_parser.add_argument(
        "--date",
        default=None,
        help="Partition date (ISO format, e.g. 2026-04-09)",
    )
    run_parser.add_argument(
        "--start-date",
        default=None,
        help="Partition window start date (ISO format). Must be used with --end-date.",
    )
    run_parser.add_argument(
        "--end-date",
        default=None,
        help="Partition window end date (ISO format). Must be used with --start-date.",
    )
    run_parser.add_argument(
        "target",
        nargs="+",
        help="Inline DAGSpec JSON (when --format inline) or one or more dotted import paths",
    )
    run_parser.set_defaults(func=_cmd_run)


def _cmd_run(args: argparse.Namespace) -> None:
    """Execute a DAG from either an inline spec or import paths.

    Raises:
        SystemExit: On invalid input, import failures, or non-zero
            execution status.
    """
    import os
    import sys

    from interloper.dag.spec import DAGSpec
    from interloper.events import EventBus, HttpEventSink, StderrEventHandler
    from interloper.runner import ExecutionStatus, build_runner
    from interloper.settings import AppSettings

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    settings = AppSettings.get()
    is_container = os.environ.get("INTERLOPER_EVENTS_TO_STDERR") == "true"

    # -- Event forwarding for child execution ---------------------------------
    # stderr: legacy path (the host scrapes container logs and re-emits).
    # http:   durable path (the child POSTs events to the ingest endpoint).
    # Both persist idempotently on the event id, so running them together
    # during rollout is safe.
    stderr_handler = None
    if is_container:
        stderr_handler = StderrEventHandler()
        EventBus.subscribe(stderr_handler)

    http_sink = None
    events_cfg = settings.events
    if args.run_id and events_cfg.ingest_url and events_cfg.ingest_token:
        http_sink = HttpEventSink(
            base_url=events_cfg.ingest_url,
            token=events_cfg.ingest_token,
            run_id=args.run_id,
        )
        EventBus.subscribe(http_sink)

    try:
        # -- Build the DAG ----------------------------------------------------
        if args.format == "inline":
            if len(args.target) != 1:
                raise SystemExit("Error: --format inline expects exactly one positional argument (the JSON spec).")
            try:
                spec = DAGSpec.model_validate_json(args.target[0])
            except Exception as exc:
                raise SystemExit(f"Error: failed to parse inline DAGSpec: {exc}") from exc
            dag = spec.reconstruct()
        else:
            dag = _dag_from_paths(args.target)

        # -- Resolve the partition --------------------------------------------
        partition = _resolve_partition(args)

        # -- Build the runner from settings -----------------------------------
        runner_cls, runner_kwargs = build_runner(settings.runner.type, settings.runner.config)

        metadata: dict[str, str] = {}
        if args.run_id:
            metadata["run_id"] = args.run_id

        materializable = [a for a in dag.assets if a.materializable]
        logger.info(
            "Running DAG with %d materializable asset(s) (%d total) using %s",
            len(materializable),
            len(dag.assets),
            runner_cls.__name__,
        )

        with runner_cls(**runner_kwargs) as runner:
            result = runner.run(dag, partition, metadata=metadata or None)

        logger.info("Run completed: %s", result.status.name)

        if result.status != ExecutionStatus.COMPLETED:
            raise SystemExit(1)

    finally:
        if stderr_handler is not None or http_sink is not None:
            EventBus.flush(timeout=5.0)
        if http_sink is not None:
            http_sink.close()
            EventBus.unsubscribe(http_sink)
        if stderr_handler is not None:
            EventBus.unsubscribe(stderr_handler)


def _dag_from_paths(paths: list[str]) -> DAG:
    """Build a DAG from dotted import paths.

    Each path is resolved via ``import_from_path`` and classified as a
    Source, Asset, or Destination.  The resolved items are passed to
    ``DAG(*items)``.

    Returns:
        A freshly-built DAG.

    Raises:
        SystemExit: If any path fails to import or resolves to an
            unsupported type.
    """
    from interloper.asset.base import Asset
    from interloper.dag.base import DAG
    from interloper.destination.base import Destination
    from interloper.source.base import Source
    from interloper.utils.imports import import_from_path

    items: list[object] = []
    for path in paths:
        try:
            obj = import_from_path(path)
        except (ImportError, AttributeError) as exc:
            raise SystemExit(f"Error: failed to import '{path}': {exc}") from exc

        if not isinstance(obj, type):
            raise SystemExit(f"Error: '{path}' did not resolve to a class")
        if not issubclass(obj, (Source, Asset, Destination)):
            raise SystemExit(f"Error: '{path}' is not a Source, Asset, or Destination subclass")

        items.append(obj())

    return DAG(*items)  # type: ignore[arg-type]


def _resolve_partition(args: argparse.Namespace) -> Partition | PartitionWindow | None:
    """Convert CLI date flags into a Partition or PartitionWindow.

    Returns:
        ``TimePartition`` for ``--date``, ``TimePartitionWindow`` for
        ``--start-date``/``--end-date``, or ``None`` if neither is set.

    Raises:
        SystemExit: If the date arguments are invalid or inconsistent.
    """
    from interloper.partitioning.time import TimePartition, TimePartitionWindow

    has_date = args.date is not None
    has_window = args.start_date is not None or args.end_date is not None

    if has_date and has_window:
        raise SystemExit("Error: --date cannot be combined with --start-date/--end-date.")

    if has_date:
        try:
            return TimePartition(dt.date.fromisoformat(args.date))
        except ValueError as exc:
            raise SystemExit(f"Error: invalid --date value: {exc}") from exc

    if has_window:
        if not (args.start_date and args.end_date):
            raise SystemExit("Error: both --start-date and --end-date must be provided.")
        try:
            start = dt.date.fromisoformat(args.start_date)
            end = dt.date.fromisoformat(args.end_date)
        except ValueError as exc:
            raise SystemExit(f"Error: invalid start/end date: {exc}") from exc
        return TimePartitionWindow(start=start, end=end)

    return None
