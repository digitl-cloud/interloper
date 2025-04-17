import argparse
import datetime as dt
import importlib.util
import logging
from typing import Any

import yaml
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from interloper.asset import Asset
from interloper.execution.observable import Event
from interloper.execution.pipeline import ExecutionStatus, ExecutionStep, Pipeline
from interloper.io.base import IO
from interloper.partitioning.partition import TimePartition
from interloper.source import Source


def _import_from_path(path: str) -> Any:
    if not any(c in path for c in (".", ":")):
        raise ValueError(f"Invalid path format: {path}. Must be either 'package.module:name' or 'package.module.name'")

    if ":" in path:
        module_path, attr_name = path.split(":")
    else:
        *module_parts, attr_name = path.split(".")
        module_path = ".".join(module_parts)
    module = importlib.import_module(module_path)
    attr = getattr(module, attr_name)

    return attr


def _import_asset_or_source_from_path(path: str) -> Source | Asset:
    attr = _import_from_path(path)
    if not isinstance(attr, Source | Asset):
        raise ValueError(f"Source or asset not found: {path}")
    return attr


def _import_io_type_from_path(path: str) -> type[IO]:
    attr = _import_from_path(path)
    if not issubclass(attr, IO):
        raise ValueError(f"IO not found: {path}")
    return attr


def _load_asset_or_source_from_spec(spec: dict) -> Source | Asset:
    source_or_asset = _import_asset_or_source_from_path(spec["path"])

    if "name" in spec:
        source_or_asset.name = spec["name"]

    if "io" in spec:
        source_or_asset.io = _load_io_from_spec(spec["io"])

    assets_args = spec.get("assets_args", {})

    if isinstance(source_or_asset, Asset):
        source_or_asset.bind(**assets_args)
    else:
        for asset in source_or_asset.assets:
            asset.bind(**assets_args)

    return source_or_asset


def _load_single_io_from_spec(spec: dict) -> IO:
    io_type = _import_io_type_from_path(spec["path"])
    kwargs = spec.get("init", {})
    return io_type(**kwargs)


def _load_io_from_spec(spec: dict) -> dict[str, IO]:
    return {io_name: _load_single_io_from_spec(io_spec) for io_name, io_spec in spec.items()}


def _load_pipeline_from_config(config: dict) -> Pipeline:
    io = _load_io_from_spec(config["io"]) if "io" in config else {}
    assets: set[Source | Asset] = set()

    pipeline_spec = config["pipeline"]
    for asset_spec in pipeline_spec:
        source_or_asset = _load_asset_or_source_from_spec(asset_spec)

        if len(source_or_asset.io) == 0:
            source_or_asset.io = io

        assets.add(source_or_asset)

    return Pipeline(list(assets))


def run(
    file: str,
    partition: str | None = None,
) -> None:
    time_partition = TimePartition(dt.date.fromisoformat(partition)) if partition else None

    with open(file) as f:
        config = yaml.safe_load(f)

    pipeline = _load_pipeline_from_config(config)

    progress = Progress(
        "{task.description}",
        SpinnerColumn(),
        TextColumn("{task.fields[status]}"),
        TimeElapsedColumn(),
    )

    # Create tasks for each asset and its steps
    asset_tasks = {}
    step_tasks = {}
    for asset in pipeline.assets.values():
        asset_tasks[asset] = progress.add_task(asset.id, status="Pending", total=1)
        step_tasks[asset] = {
            ExecutionStep.EXECUTION: progress.add_task("  Execution", status="Pending", total=1, visible=False),
            ExecutionStep.NORMALIZATION: progress.add_task("  Normalization", status="Pending", total=1, visible=False),
            ExecutionStep.MATERIALIZATION: progress.add_task(
                "  Materialization", status="Pending", total=1, visible=False
            ),
        }

    def on_pipeline_event(pipeline: Pipeline, event: Event) -> None:
        if not isinstance(event.observable, Asset):
            return

        asset = event.observable
        assert asset in asset_tasks

        if event.status == ExecutionStatus.RUNNING:
            progress.update(asset_tasks[asset], status="Running...")
            for step_task in step_tasks[asset].values():
                progress.update(step_task, visible=True)
        elif event.status == ExecutionStatus.SUCCESS:
            progress.update(asset_tasks[asset], advance=1, status="[green]Complete")
            for step_task in step_tasks[asset].values():
                progress.update(step_task, visible=False)
        elif event.status == ExecutionStatus.FAILURE:
            progress.update(asset_tasks[asset], status="[red]Failed")
            for step_task in step_tasks[asset].values():
                progress.update(step_task, visible=False)

        if event.step in step_tasks[asset]:
            step_task = step_tasks[asset][event.step]
            if event.status == ExecutionStatus.RUNNING:
                progress.update(step_task, status="Running...")
            elif event.status == ExecutionStatus.SUCCESS:
                progress.update(step_task, advance=1, status="[green]Complete")
            elif event.status == ExecutionStatus.FAILURE:
                progress.update(step_task, status="[red]Failed")

    pipeline.on_event_callback = on_pipeline_event

    with Live(progress):
        pipeline.materialize(partition=time_partition)


def main() -> None:
    logging.disable()

    parser = argparse.ArgumentParser(description="Interloper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("file", type=str, help="Path to script, or config if --from-config is provided")
    run_parser.add_argument("--partition", type=str, help="Partition to materialize (YYYY-MM-DD)")

    # Visualize
    subparsers.add_parser("viz", help="Visualize pipeline materialization")

    args = parser.parse_args()

    if args.command == "run":
        run(args.file, args.partition)
    # elif args.command == "viz":
    #     viz()


if __name__ == "__main__":
    main()
