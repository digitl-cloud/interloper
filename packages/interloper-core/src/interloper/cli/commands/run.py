"""``interloper run`` — execute a DAG directly using the configured runner.

Framework-level execution with no database or persistence layer.  Used
both for direct user invocation and as the entry point that
:class:`interloper_docker.DockerRunner` calls inside spawned containers.

Input formats
-------------

* ``--file/-f <manifest.yaml>`` — a declarative run manifest
  (:class:`~interloper.manifest.RunManifest`): sources/assets with their
  config, destinations, an optional runner override, and a partition.
* ``--format inline <json>`` — a serialized :class:`DAGSpec` as JSON.
  This is the mode used by the ``DockerRunner`` to pass a mini-DAG to a
  child container.
* ``--format paths <path>...`` — one or more dotted import paths that
  resolve to Source, Asset, or Destination classes.  The resolved
  classes are instantiated and passed to ``DAG(*items)``.

The runner is the one configured in ``AppSettings.runner`` (top-level
``runner`` key in ``interloper.yaml`` or ``INTERLOPER_RUNNER_*``
environment variables), unless a manifest provides its own ``runner``
block.  Likewise the CLI date flags take precedence over a manifest's
``partition`` block.

``--dry-run`` validates and prints the resolved plan (assets grouped by
execution generation, runner, partition) without materializing anything —
useful for checking a curated set of manifests.

During execution, run/asset lifecycle events and ``context.logger``
messages flow through the standard logging stack (via
:class:`~interloper.events.console.ConsoleEventHandler`), sharing one
format and stream with regular log lines.  Verbosity is the logging
level: ``-v`` shows DEBUG (execution / destination-I/O events), ``-q``
shows only warnings and errors.  ``--events json`` streams raw event
JSON lines to stdout instead, unaffected by the level.  In container
mode (``INTERLOPER_EVENTS_TO_STDERR``) the ``@EVENT:`` forwarding lines
are emitted instead.
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
    subparsers: argparse._SubParsersAction,
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
        "-f",
        "--file",
        default=None,
        help="Path to a declarative run manifest (YAML). Mutually exclusive with positional targets.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the resolved run plan without executing",
    )
    run_parser.add_argument(
        "--events",
        choices=["pretty", "json"],
        default="pretty",
        help="Event output: 'pretty' renders events through the logger (default); "
        "'json' streams raw event JSON lines to stdout",
    )
    run_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only show warnings and errors (takes precedence over --verbose; "
        "does not affect --events json output)",
    )
    run_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Debug verbosity: include execution and destination I/O events",
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
        nargs="*",
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
    from interloper.events import EventBus, StderrEventHandler
    from interloper.runner import ExecutionStatus, build_runner
    from interloper.settings import AppSettings

    # One logging setup for everything the command prints: framework log
    # lines and run events share the same format, stream, and level.
    # ``force=True`` rebinds the handler to the current stderr on every
    # invocation (repeated in-process calls, captured streams in tests).
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
        force=True,
    )

    settings = AppSettings.get()
    is_container = os.environ.get("INTERLOPER_EVENTS_TO_STDERR") == "true"

    # -- Subscribe stderr event handler (for container event forwarding) ------
    stderr_handler = None
    if is_container:
        stderr_handler = StderrEventHandler()
        EventBus.subscribe(stderr_handler)

    try:
        # -- Build the DAG ----------------------------------------------------
        plan = None
        if args.file is not None:
            if args.target:
                raise SystemExit("Error: --file cannot be combined with positional targets.")
            from interloper.errors import ManifestError
            from interloper.manifest import RunManifest

            try:
                plan = RunManifest.from_yaml_file(args.file).compile()
            except ManifestError as exc:
                raise SystemExit(f"Error: {exc}") from exc
            dag = plan.dag
        elif args.format == "inline":
            if len(args.target) != 1:
                raise SystemExit("Error: --format inline expects exactly one positional argument (the JSON spec).")
            try:
                spec = DAGSpec.model_validate_json(args.target[0])
            except Exception as exc:
                raise SystemExit(f"Error: failed to parse inline DAGSpec: {exc}") from exc
            dag = spec.reconstruct()
        else:
            if not args.target:
                raise SystemExit("Error: provide one or more import paths, or a manifest via --file.")
            dag = _dag_from_paths(args.target)

        # -- Resolve the partition (CLI flags win over the manifest) ----------
        partition = _resolve_partition(args)
        if partition is None and plan is not None:
            partition = plan.partition

        # -- Build the runner (manifest override wins over settings) ----------
        if plan is not None and plan.runner is not None:
            runner_cls, runner_kwargs = build_runner(plan.runner.type, plan.runner.config)
        else:
            runner_cls, runner_kwargs = build_runner(settings.runner.type, settings.runner.config)

        # -- Dry run: print the plan and stop ----------------------------------
        if args.dry_run:
            _print_plan(dag, partition, runner_cls.__name__, plan.name if plan else "")
            return

        # -- Console event output (interactive mode only) ----------------------
        # Events become records on the ``interloper.run`` logger, so the
        # basicConfig above governs their visibility and format.  In
        # container mode the StderrEventHandler already owns stderr with
        # ``@EVENT:`` lines for the host to re-parse; printing on the host
        # side covers child events too, since cross-process runners re-emit
        # them on the host bus.
        on_event = None
        if not is_container:
            from interloper.events import ConsoleEventHandler

            on_event = ConsoleEventHandler(json_lines=args.events == "json")

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

        with runner_cls(**runner_kwargs, on_event=on_event) as runner:
            result = runner.run(dag, partition, metadata=metadata or None)

        logger.info("Run completed: %s", result.status.name)

        if result.status != ExecutionStatus.COMPLETED:
            raise SystemExit(1)

    finally:
        if stderr_handler is not None:
            EventBus.flush(timeout=5.0)
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

    return DAG(*items)  # ty: ignore[invalid-argument-type]


def _print_plan(
    dag: DAG,
    partition: Partition | PartitionWindow | None,
    runner_name: str,
    name: str,
) -> None:
    """Print the resolved run plan to stdout without executing it."""
    materializable = [a for a in dag.assets if a.materializable]
    lines: list[str] = []
    if name:
        lines.append(f"Run:       {name}")
    lines.append(f"Runner:    {runner_name}")
    lines.append(f"Partition: {partition if partition is not None else '(none)'}")
    lines.append(f"Assets:    {len(materializable)} materializable / {len(dag.assets)} total")
    lines.append("")
    for level, generation in enumerate(dag.topological_generations(), start=1):
        lines.append(f"  {level}. {', '.join(asset.qualified_key for asset in generation)}")
    print("\n".join(lines))


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
