import argparse
import datetime as dt
import importlib.util
import logging
from enum import Enum
from time import sleep
from typing import Any

import yaml
from jsonschema import ValidationError, validate
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.threading import ThreadingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TaskProgressColumn, TextColumn

import interloper as itlp
from interloper.asset import Asset

# from interloper.execution.observable import Event, ExecutionStatus, ExecutionStep
from interloper.execution.pipeline import Pipeline
from interloper.io.base import IO

# from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow
from interloper.source import Source

_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$defs": {
        "io": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "init": {"type": "object"},
            },
            "required": ["path", "init"],
            "additionalProperties": False,
        },
        "source": {
            "type": "object",
            "properties": {
                "type": {"const": "source"},
                "name": {"type": "string"},
                "path": {"type": "string"},
                "assets": {"type": "array", "items": {"type": "string"}},
                "assets_args": {"type": "object"},
                "io": {"$ref": "#/$defs/io"},
            },
            "required": ["name", "type", "path"],
            "additionalProperties": False,
        },
        "asset": {
            "type": "object",
            "properties": {
                "type": {"const": "asset"},
                "name": {"type": "string"},
                "path": {"type": "string"},
                "args": {"type": "object"},
                "io": {"$ref": "#/$defs/io"},
            },
            "required": ["name", "type", "path"],
            "additionalProperties": False,
        },
    },
    "type": "object",
    "properties": {
        "io": {
            "type": "object",
            "additionalProperties": {"$ref": "#/$defs/io"},
        },
        "pipeline": {
            "type": "array",
            "items": {
                "oneOf": [
                    {"$ref": "#/$defs/source"},
                    {"$ref": "#/$defs/asset"},
                ]
            },
        },
    },
    "required": ["io", "pipeline"],
}


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


def _load_source_from_spec(spec: dict) -> Source:
    source = _import_from_path(spec["path"])
    if not isinstance(source, Source):
        raise ValueError(f"Source not found: {spec['path']}")

    if "name" in spec:
        source.name = spec["name"]

    if "io" in spec:
        source.io = _load_io_from_spec(spec["io"])

    if "assets" in spec:
        source.materializable = False
        for asset_name in spec["assets"]:
            source[asset_name].materializable = True

    assets_args = spec.get("assets_args", {})
    for asset in source.assets:
        asset.bind(**assets_args, ignore_unknown_params=True)

    return source


def _load_asset_from_spec(spec: dict) -> Asset:
    asset = _import_from_path(spec["path"])
    if not isinstance(asset, Asset):
        raise ValueError(f"Asset not found: {spec['path']}")

    if "name" in spec:
        asset.name = spec["name"]

    if "io" in spec:
        asset.io = _load_io_from_spec(spec["io"])

    if "args" in spec:
        asset.bind(**spec["args"], ignore_unknown_params=True)

    return asset


def _load_single_io_from_spec(spec: dict) -> IO:
    io_type = _import_from_path(spec["path"])
    if not issubclass(io_type, IO):
        raise ValueError(f"IO not found: {spec['path']}")

    kwargs = spec.get("init", {})
    return io_type(**kwargs)


def _load_io_from_spec(spec: dict) -> dict[str, IO]:
    return {io_name: _load_single_io_from_spec(io_spec) for io_name, io_spec in spec.items()}


def _load_pipeline_from_config(config: dict) -> Pipeline:
    io = _load_io_from_spec(config["io"]) if "io" in config else {}
    assets: set[Source | Asset] = set()

    pipeline_spec = config["pipeline"]
    for asset_spec in pipeline_spec:
        if asset_spec["type"] == "source":
            source_or_asset = _load_source_from_spec(asset_spec)
        else:
            source_or_asset = _load_asset_from_spec(asset_spec)

        if len(source_or_asset.io) == 0:
            source_or_asset.io = io

        assets.add(source_or_asset)

    return Pipeline(list(assets), async_events=True)


def _load_and_validate_config(file: str) -> dict:
    with open(file) as f:
        config = yaml.safe_load(f)

    try:
        validate(config, _schema)
    except ValidationError as e:
        raise ValueError(f"Invalid config: {e.message}")

    return config


########################
# TESTING
########################
@itlp.source
def my_source_1() -> tuple[itlp.Asset, ...]:
    @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
    def my_asset_A() -> str:
        sleep(1.6)
        return "A"

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
    def my_asset_B() -> str:
        sleep(1.3)
        return "B"

    return (my_asset_A, my_asset_B)


@itlp.source
def my_source_2() -> tuple[itlp.Asset, ...]:
    @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
    def my_asset_C() -> str:
        sleep(1.1)
        return "C"

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
    def my_asset_D() -> str:
        sleep(1.5)
        return "D"

    return (my_asset_C, my_asset_D)


io: dict[str, IO] = {"file": itlp.FileIO(base_dir="data")}
my_source_1.io = io
my_source_2.io = io
pipeline = Pipeline([my_source_1, my_source_2], async_events=True)
########################
########################


def materialize(
    file: str,
    partition: str | None = None,
) -> None:
    # config = _load_and_validate_config(file)
    # pipeline = _load_pipeline_from_config(config)
    # time_partition = TimePartition(dt.date.fromisoformat(partition)) if partition else None
    time_partitions = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 10))

    progress = Progress(
        "{task.description}",
        BarColumn(),
        SpinnerColumn(),
        TextColumn("{task.fields[status]}"),
        TaskProgressColumn(),
        # TimeElapsedColumn(),
    )

    source_tasks: dict[str | None, TaskID] = {}  # source_id -> task_id
    asset_tasks: dict[str, TaskID] = {}  # asset_id -> task_id
    step_tasks: dict[str, TaskID] = {}  # asset_id -> task_id

    # Create source and asset tasks
    for source_id, assets in pipeline.group_assets_by_source().items():
        source_tasks[source_id] = progress.add_task(
            f"[bold magenta]{source_id}",
            status="",
            total=len(assets) * time_partitions.partition_count(),
            visible=True,
        )
        for asset in assets:
            symbols = ("└", " ") if asset == assets[-1] else ("├", "│")
            asset_tasks[asset.id] = progress.add_task(
                f"  {symbols[0]}─[bold cyan]{asset.name}",
                status="",
                total=time_partitions.partition_count(),
                visible=False,
            )
            step_tasks[asset.id] = progress.add_task(
                f"  {symbols[1]} └─{time_partitions.start.isoformat()}",
                status="",
                total=time_partitions.partition_count(),
                visible=False,
            )

    class Op(Enum):
        MATERIALIZATION = "interloper.asset.materialize"
        EXECUTE = "interloper.asset.execute"
        NORMALIZE = "interloper.asset.normalize"
        WRITE = "interloper.asset.write"

    class CLISpanProcessor(SpanProcessor):
        def __init__(self): ...

        def on_start(self, span: Span, parent_context: Context | None = None) -> None:
            if not span.name.startswith("interloper.asset"):
                return

            assert span.attributes
            op = Op(span.name)
            asset_id = str(span.attributes["asset_id"])

            if op == Op.MATERIALIZATION:
                progress.update(asset_tasks[asset_id], status="", visible=True)
                progress.update(step_tasks[asset_id], status="", completed=0, visible=True)

            if op in (Op.EXECUTE, Op.NORMALIZE, Op.WRITE):
                progress.update(step_tasks[asset_id], status=f"[yellow]{op.value.split('.')[-1]}")

        def on_end(self, span: ReadableSpan) -> None:
            if not span.name.startswith("interloper.asset"):
                return

            assert span.attributes
            op = Op(span.name)
            asset_id = str(span.attributes["asset_id"])
            source_id = str(span.attributes["source_name"]) if "source_name" in span.attributes else None

            if op in (Op.EXECUTE, Op.NORMALIZE, Op.WRITE):
                if span.status.is_ok:
                    progress.update(step_tasks[asset_id], advance=1)
                else:
                    progress.update(step_tasks[asset_id], status=f"[red]Failed ({span.name.split('.')[-1]})")

            elif op == Op.MATERIALIZATION:
                if span.status.is_ok:
                    progress.update(source_tasks[source_id], advance=1)
                    progress.update(asset_tasks[asset_id], advance=1, status="[green]Complete")
                    progress.update(step_tasks[asset_id], status="[green]Complete")
                else:
                    progress.update(asset_tasks[asset_id], status="[red]Failed")

    ThreadingInstrumentor().instrument()
    cli_span_processor = CLISpanProcessor()
    batch_span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
    resource = Resource(attributes={"service.name": "interloper"})
    trace_provider = TracerProvider(resource=resource)
    trace_provider.add_span_processor(cli_span_processor)
    trace_provider.add_span_processor(batch_span_processor)
    trace.set_tracer_provider(trace_provider)

    console = Console()
    with Live(progress, console=console, refresh_per_second=10):
        try:
            pipeline.backfill(partitions=time_partitions)
        except Exception:
            pass
        # finally:
        #     # Final update of the progress
        #     for asset, state in pipeline.get_completed_assets():
        #         progress.update(asset_tasks[asset.id], status="[green]Complete")
        #     # for step_task in step_tasks.values():
        #     #     progress.update(step_task, visible=False)

    # Display failed assets
    failed_assets = pipeline.get_failed_assets()
    if failed_assets:
        console.print("\n[red bold]Failed Assets:[/red bold]")
        for asset, state in failed_assets:
            console.print(Panel(str(state.error), title=f"[red]{asset.id}[/red]", expand=False, title_align="left"))

    trace_provider.shutdown()


def main() -> None:
    logging.disable()

    parser = argparse.ArgumentParser(description="Interloper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Materialize
    materialize_parser = subparsers.add_parser("materialize", help="Materialize a pipeline")
    materialize_parser.add_argument("file", type=str, help="Path to script, or config if --from-config is provided")
    materialize_parser.add_argument("--partition", type=str, help="Partition to materialize (YYYY-MM-DD)")

    # Visualize
    subparsers.add_parser("viz", help="Visualize pipeline materialization")

    args = parser.parse_args()

    if args.command == "materialize":
        materialize(args.file, args.partition)
    # elif args.command == "viz":
    #     viz()


if __name__ == "__main__":
    main()
