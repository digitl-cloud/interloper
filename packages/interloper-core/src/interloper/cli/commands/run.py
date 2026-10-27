"""``interloper run`` — execute a DAG directly using the configured runner.

Framework-level execution with no database or persistence layer.  Used
both for direct user invocation and as the entry point that
`interloper_docker.DockerRunner` calls inside spawned containers.

Input formats
-------------

* ``--file/-f <spec.yaml>`` — a component spec document
  (`Spec`, ``{key|path, init}``)
  for any **runnable** component: the run is literally "reconstruct the
  component and run its DAG".  A Job spec is the composite form — targets
  with workload-level default destinations and resources.
* ``--format inline <json>`` — a serialized `DAGSpec` as JSON, or
  ``@<path>`` to read it from a file. This is how a launcher hands a run's
  pod its DAG, and a container runner a child its mini-DAG.
* ``--format paths <path>...`` — one or more dotted import paths that
  resolve to Source or Asset classes, built into a DAG by `DAG.from_paths`.

The runner is the one configured in ``AppSettings.runner`` (top-level
``runner`` key in ``interloper.yaml`` or ``INTERLOPER_RUNNER_*``
environment variables); the partition comes from the CLI date flags.

``--dry-run`` validates and prints the resolved plan (operations grouped by
execution generation, runner, partition) without materializing anything —
useful for checking a curated set of workload files.

During execution, run/operation lifecycle events and ``context.logger``
messages flow through the standard logging stack (via
`ConsoleEventHandler`), sharing one
format and stream with regular log lines.  Verbosity is the logging
level: ``-v`` shows DEBUG (execution / destination-I/O events), ``-q``
shows only warnings and errors.  ``--events json`` streams raw event
JSON lines to stdout instead, unaffected by the level.

A platform run's pod runs this command with ``INTERLOPER_RUN_TOKEN`` set: the
run's events are then reported to the platform through an `EventRecorder`, and
``--metadata`` carries the run's ids and names into every event. A child a
container runner starts for one attempt at an operation of its host's run
carries ``INTERLOPER_RUN_OPERATION``: it makes that one attempt, reports only
what happens inside the operation, and leaves a failed attempt's verdict in
its termination message for the host, which owns the operation's lifecycle.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from interloper.events import EventClient

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.partitioning.base import PartitionWindow

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
        description="Execute a DAG directly with the configured runner, without persistence. "
        "The exit status is non-zero on invalid input, an import failure, or a run that did not complete.",
    )
    source = run_parser.add_mutually_exclusive_group()
    source.add_argument(
        "--format",
        choices=["inline", "paths"],
        default="paths",
        help="Input format: 'inline' for a DAGSpec JSON string, 'paths' for dotted import paths",
    )
    source.add_argument(
        "-f",
        "--file",
        default=None,
        help="Path to a runnable component spec (YAML). Mutually exclusive with positional targets.",
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
        help="Only show warnings and errors (takes precedence over --verbose; does not affect --events json output)",
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
        "--metadata",
        default=None,
        help="Run metadata as a JSON object, spread into every event (ids and names, never secrets)",
    )
    run_parser.add_argument(
        "--date",
        default=None,
        help="Partition key (e.g. 2026-04-09; also 2026-04, 2026, or 2026-04-09T13)",
    )
    run_parser.add_argument(
        "--start-date",
        default=None,
        help="Partition window start key. Must be used with --end-date and share its granularity.",
    )
    run_parser.add_argument(
        "--end-date",
        default=None,
        help="Partition window end key. Must be used with --start-date and share its granularity.",
    )
    run_parser.add_argument(
        "target",
        nargs="*",
        help="Inline DAGSpec JSON or @<path> to a file holding it (when --format inline), or dotted import paths",
    )
    run_parser.set_defaults(handler=_cmd_run)


def _cmd_run(args: argparse.Namespace) -> None:
    """Execute a DAG from either an inline spec or import paths.

    Args:
        args: Parsed CLI arguments, carrying the DAG input (``file``, ``format``,
            ``target``), the partition flags, and the output controls
            (``dry_run``, ``events``, ``quiet``, ``verbose``, ``run_id``).

    Raises:
        SystemExit: On invalid input, import failures, a recorded run that
            was lost, or non-zero execution status.
    """
    from interloper.cli.console import configure_console_logging
    from interloper.errors import PartitionError, RunLostError, format_exception
    from interloper.events import ConsoleEventHandler, Event, EventRecorder, EventType
    from interloper.operation import OperationResult
    from interloper.partitioning import PartitionWindow, TimePartition
    from interloper.runner import ExecutionStatus, Runner
    from interloper.settings import AppSettings

    # One logging setup for everything the command prints: framework log
    # lines and run events share the same format, stream, and level.
    settings = AppSettings.get()
    configure_console_logging(
        logging.WARNING if args.quiet else logging.DEBUG if args.verbose else logging.INFO,
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        "%H:%M:%S",
        log_format=settings.log.format,
    )
    token = os.environ.get("INTERLOPER_RUN_TOKEN")
    attempted = os.environ.get("INTERLOPER_RUN_OPERATION") or None
    if token is not None and not args.run_id:
        raise SystemExit("Error: a recorded run needs --run-id.")

    # A failure to even start is the run's own failure, or a child's attempt's: neither process holds a store.
    recorder = None
    if token is not None:
        recorder = EventRecorder(
            EventClient(settings.server.internal_url, token, args.run_id).ingest,
            run_id=args.run_id,
            heartbeat_interval=settings.reaper.heartbeat_interval,
            heartbeat_timeout=settings.reaper.heartbeat_timeout,
            on_lost=_exit_lost,
            component_id=attempted,
        )

    try:
        dag = _build_dag(args)
        if recorder is None:
            window = _resolve_window(args, dag)
        else:
            # A platform run's window is its partition key, read as the platform reads it.
            window = PartitionWindow.of(TimePartition.from_key(args.date)) if args.date else None
        runner = Runner.from_settings(settings.runner)
        if attempted is not None:
            dag.operation_map[attempted].retry = None
        metadata: dict[str, Any] = json.loads(args.metadata) if args.metadata else {}
    except (Exception, SystemExit) as error:
        reason = str(error) if isinstance(error, SystemExit) else format_exception(error)
        if attempted is not None:
            OperationResult(error=reason).write_termination_message()
        elif recorder is not None:
            ending = {"run_id": args.run_id, "error": reason, "message": f"Run failed: {reason}"}
            with recorder:
                recorder(Event(type=EventType.RUN_FAILED, metadata=ending))
        raise
    if args.run_id:
        metadata["run_id"] = args.run_id

    if args.dry_run:
        _print_plan(dag, window, type(runner).__name__)
        return

    enabled = [operation for operation in dag.operations if operation.enabled]
    logger.info(
        "Running DAG with %d enabled operation(s) (%d total) using %s",
        len(enabled),
        len(dag.operations),
        type(runner).__name__,
    )

    try:
        if recorder is None:
            # Events become records on the ``interloper.run`` logger, so the console logging set up above
            # governs their visibility and format.
            runner.on_event = ConsoleEventHandler(json_lines=args.events == "json")
            result = asyncio.run(runner.run(dag, window, metadata=metadata or None))
        else:
            with recorder:
                recorded = runner.model_copy(update={"on_event": recorder, "reraise": False})
                result = asyncio.run(recorded.run(dag, window, metadata))
                recorder.finish(result)
    except Exception as error:
        if attempted is not None:
            OperationResult(error=format_exception(error)).write_termination_message()
        if isinstance(error, (PartitionError, RunLostError)):
            raise SystemExit(f"Error: {error}") from error
        raise

    if attempted is not None and (failed := result.executions[attempted].result) and not failed.succeeded:
        failed.write_termination_message()

    logger.info("Run completed: %s", result.status.name)

    if result.status != ExecutionStatus.COMPLETED:
        raise SystemExit(1)


# -- Internals -----------------------------------------------------------------


def _build_dag(args: argparse.Namespace) -> DAG:
    """Build the DAG the command runs, from a spec file, an inline spec or import paths.

    Args:
        args: Parsed CLI arguments, carrying ``file``, ``format`` and ``target``.

    Returns:
        The DAG.

    Raises:
        SystemExit: On invalid input or a spec or path that does not resolve.
    """
    from interloper.dag import DAG, DAGSpec
    from interloper.errors import DAGError, SpecError

    if args.file is not None:
        if args.target:
            raise SystemExit("Error: --file cannot be combined with positional targets.")
        try:
            return DAG.from_spec_file(args.file)
        except (SpecError, DAGError) as error:
            raise SystemExit(f"Error: {error}") from error
        except Exception as error:
            raise SystemExit(f"Error: failed to reconstruct spec '{args.file}': {error}") from error
    if args.format == "inline":
        if len(args.target) != 1:
            raise SystemExit("Error: --format inline expects exactly one positional argument (the JSON spec).")
        document = args.target[0]
        try:
            if document.startswith("@"):
                document = Path(document[1:]).read_text()
            spec = DAGSpec.model_validate_json(document)
        except Exception as error:
            raise SystemExit(f"Error: failed to parse inline DAGSpec: {error}") from error
        return DAG.from_spec(spec)
    if not args.target:
        raise SystemExit("Error: provide one or more import paths, or a spec via --file.")
    try:
        return DAG.from_paths(args.target)
    except DAGError as error:
        raise SystemExit(f"Error: {error}") from error


def _exit_lost() -> None:
    """Exit this run's process at once: its run was ended elsewhere, or can no longer report.

    The process exists for this one run, and an operation blocked in sync
    code cannot be interrupted any other way. The recorder has delivered
    what it held before calling this.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)


def _print_plan(dag: DAG, window: PartitionWindow | None, runner_name: str) -> None:
    """Print the resolved run plan to stdout without executing it.

    Args:
        dag: The resolved DAG, printed as its topological generations.
        window: The window to run for; ``None`` prints as ``(none)``.
        runner_name: Class name of the configured runner.
    """
    enabled = [operation for operation in dag.operations if operation.enabled]
    lines = [
        f"Runner:    {runner_name}",
        f"Window:    {window if window is not None else '(none)'}",
        f"Operations: {len(enabled)} enabled / {len(dag.operations)} total",
        "",
    ]
    for level, generation in enumerate(dag.topological_generations(), start=1):
        lines.append(f"  {level}. {', '.join(operation.qualified_key for operation in generation)}")
    print("\n".join(lines))


def _resolve_window(args: argparse.Namespace, dag: DAG) -> PartitionWindow | None:
    """Resolve the CLI date flags into the run's window, through the DAG's partitioning.

    The keys are parsed by the partitioning of the DAG's first enabled partitioned operation,
    so a key means what that asset means (a day key for a daily asset, a declared value for a
    list-partitioned one); the DAG already enforces that every edge shares a partitioning, and
    the runner's preflight checks the window against every operation.

    Args:
        args: Parsed CLI arguments, carrying ``date``, ``start_date`` and ``end_date``.
        dag: The DAG about to run.

    Returns:
        The window for ``--date`` (one partition) or ``--start-date``/``--end-date``, or ``None``
        when neither is set.

    Raises:
        SystemExit: If the flags are inconsistent, no operation is partitioned, or a key does
            not name a partition of the DAG's partitioning.
    """
    from interloper.errors import PartitionError

    has_date = args.date is not None
    has_window = args.start_date is not None or args.end_date is not None
    if has_date and has_window:
        raise SystemExit("Error: --date cannot be combined with --start-date/--end-date.")
    if not has_date and not has_window:
        return None
    if has_window and not (args.start_date and args.end_date):
        raise SystemExit("Error: both --start-date and --end-date must be provided.")

    partitioning = next(
        (operation.partitioning for operation in dag.operations if operation.enabled and operation.partitioning),
        None,
    )
    if partitioning is None:
        raise SystemExit("Error: a partition key was given, but no enabled operation of the DAG is partitioned.")
    start_key, end_key = (args.date, args.date) if has_date else (args.start_date, args.end_date)
    try:
        return partitioning.window(start_key, end_key)
    except PartitionError as error:
        raise SystemExit(f"Error: {error}") from error
