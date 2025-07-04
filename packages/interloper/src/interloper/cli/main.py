"""This module contains the main CLI for Interloper."""

import argparse
import datetime as dt
import logging
from typing import Any

import yaml
from rich.live import Live

from interloper.cli.visualizer import MaterializationVisualizer
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.events.bus import get_event_bus
from interloper.events.event import Event, EventType
from interloper.execution.execution import Execution, MultiThreadExecution
from interloper.execution.state import ExecutionStatus
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow

event_bus = get_event_bus()


def _load_script(path: str) -> DAG:
    script_globals: dict[str, Any] = {}
    with open(path) as f:
        exec(f.read(), script_globals)

    dags = [obj for obj in script_globals.values() if isinstance(obj, DAG)]
    if not dags:
        raise ValueError(f"No DAG objects found in script {path}")
    if len(dags) > 1:
        raise ValueError(f"Multiple DAG objects found in script {path}")

    return dags[0]


def _visualize(execution: Execution) -> None:
    visualizer = MaterializationVisualizer()
    errors = []

    with Live(refresh_per_second=10) as live:

        def on_event(event: Event) -> None:
            if event.type == EventType.ASSET_MATERIALIZATION:
                if event.status == ExecutionStatus.FAILED:
                    errors.append(event.error)
                live.update(visualizer.render_all(execution.state_by_source, errors))

        event_bus.subscribe(on_event, is_async=True)
        execution()
        live.update(visualizer.render_all(execution.state_by_source, errors))
        event_bus.unsubscribe(on_event)


def run(
    path: str,
    date: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> None:
    """Run a DAG from a script.

    Args:
        path: The path to the script.
        date: The date to run the DAG for.
        start_date: The start date of the time window.
        end_date: The end date of the time window.

    Raises:
        ValueError: If both date and a time window are specified.
    """
    dag = _load_script(path)

    if date is not None and (start_date is not None or end_date is not None):
        raise ValueError("Cannot specify both --date and --start-date/--end-date")

    if date is not None:
        partitions = TimePartition(date)
    elif start_date is not None and end_date is not None:
        partitions = TimePartitionWindow(start=start_date, end=end_date)
    else:
        partitions = None

    execution = MultiThreadExecution(dag, partitions)
    _visualize(execution)


def load(
    path: str,
    date: dt.date | None = None,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    iter_partitions: bool = False,
) -> None:
    """Load a DAG from a config file.

    Args:
        path: The path to the config file.
        date: The date to run the DAG for.
        start_date: The start date of the time window.
        end_date: The end date of the time window.
        iter_partitions: Whether to iterate over partitions.

    Raises:
        ValueError: If both date and a time window are specified.
    """
    with open(path) as f:
        config = yaml.safe_load(f)
        dag = DAGSpec.model_validate(config).to_dag()

    if date is not None and (start_date is not None or end_date is not None):
        raise ValueError("Cannot specify both --date and --start-date/--end-date")

    if date is not None:
        partitions = TimePartition(date)
    elif start_date is not None and end_date is not None:
        partitions = TimePartitionWindow(start=start_date, end=end_date)
        if iter_partitions:
            partitions = list(partitions.iter_partitions())
    else:
        partitions = None

    execution = MultiThreadExecution(dag, partitions)
    _visualize(execution)


def main() -> None:
    """The main entrypoint for the CLI."""
    logging.disable()

    parser = argparse.ArgumentParser(description="Interloper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run command
    run_parser = subparsers.add_parser("run", help="Executes a script declaring a DAG and materializes it")
    run_parser.add_argument("script", type=str, help="Python script containing a DAG definition")
    run_parser.add_argument("--date", type=dt.date.fromisoformat, help="Single date to materialize")
    run_parser.add_argument("--start-date", type=dt.date.fromisoformat, help="Start date for date range")
    run_parser.add_argument("--end-date", type=dt.date.fromisoformat, help="End date for date range")

    # Load command
    load_parser = subparsers.add_parser("load", help="Loads a DAG from a configuration file and materializes it")
    load_parser.add_argument("config", type=str, help="YAML configuration file containing a DAG definition")
    load_parser.add_argument("--date", type=dt.date.fromisoformat, help="Single date to materialize")
    load_parser.add_argument("--start-date", type=dt.date.fromisoformat, help="Start date for date range")
    load_parser.add_argument("--end-date", type=dt.date.fromisoformat, help="End date for date range")
    load_parser.add_argument("--iter-partitions", action="store_true", help="Iterate over partitions")

    args = parser.parse_args()
    if args.command == "run":
        run(args.script, args.date, args.start_date, args.end_date)
    elif args.command == "load":
        load(args.config, args.date, args.start_date, args.end_date, args.iter_partitions)


if __name__ == "__main__":
    main()
