"""``interloper run``: execute a DAG with the configured runner, without persistence.

What to execute
---------------

* import paths (positional, the default): dotted paths to Source or Asset
  classes, built into a DAG by `DAG.from_paths`.
* ``--file/-f <spec.yaml>``: a component spec document (`Spec`,
  ``{key|path, init}``) for any **runnable** component: the run is literally
  "reconstruct the component and run its DAG". A Job spec is the composite
  form: targets with workload-level default destinations and resources.
* ``--format inline <json>``: a serialized `DAGSpec` as JSON, or ``@<path>``
  to read it from a file.

``--operation-id <id>`` executes one operation of that DAG for a host run
whose runner executes each operation in its own process (the Docker and
Kubernetes runners, outside the platform): its upstreams are read, not
executed; it executes once, since the host owns its retries; and what it came
to is left in the termination message the host reads back. A platform run is
executed by its id instead, with ``interloper launch``.

The window comes from the date flags. The runner is the one configured in
``AppSettings.runner`` (top-level ``runner`` key in ``interloper.yaml`` or
``INTERLOPER_RUNNER_*`` environment variables).

``--dry-run`` validates and prints the resolved plan (operations grouped by
execution generation, runner, partition) without materializing anything,
useful for checking a curated set of workload files.

During execution, run/operation lifecycle events and ``context.logger``
messages flow through the standard logging stack (via
`ConsoleEventHandler`), sharing one format and stream with regular log lines.
Verbosity is the logging level: ``-v`` shows DEBUG (execution / destination-I/O
events), ``-q`` shows only warnings and errors. ``--events json`` streams raw
event JSON lines to stdout instead, unaffected by the level.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import TYPE_CHECKING

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
        "--operation-id",
        default=None,
        help="Execute only this operation of the DAG, once, its upstreams read rather than executed, leaving what "
        "it came to for the host run that started this process",
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
    """Execute a DAG from import paths, a spec file or an inline spec.

    Args:
        args: Parsed CLI arguments, carrying what to execute (``target``,
            ``file``, ``format``, ``operation_id``), the partition flags, and
            the output controls (``dry_run``, ``events``, ``quiet``, ``verbose``).

    Raises:
        SystemExit: On invalid input, import failures, or non-zero execution status.
    """
    from interloper.cli.commands.execution import configure_logging, execute, for_operation, leave_failure
    from interloper.errors import format_exception
    from interloper.events import ConsoleEventHandler
    from interloper.runner import Runner
    from interloper.settings import AppSettings

    configure_logging(quiet=args.quiet, verbose=args.verbose)
    try:
        dag = _build_dag(args)
        window = _resolve_window(args, dag)
        if args.operation_id is not None:
            dag, window = for_operation(dag, window, args.operation_id)
        runner = Runner.from_settings(AppSettings.get().runner)
    except (Exception, SystemExit) as error:
        leave_failure(args.operation_id, str(error) if isinstance(error, SystemExit) else format_exception(error))
        raise

    if args.dry_run:
        _print_plan(dag, window, type(runner).__name__)
        return

    # Events become records on the ``interloper.run`` logger, so the console logging set up above
    # governs their visibility and format.
    runner.on_event = ConsoleEventHandler(json_lines=args.events == "json")
    execute(runner, dag, window, {}, operation_id=args.operation_id)


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
