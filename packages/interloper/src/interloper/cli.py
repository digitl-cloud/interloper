import argparse
import datetime as dt
import importlib.util
import logging
from typing import Any

import yaml

from interloper.asset import Asset
from interloper.execution.pipeline import Pipeline
from interloper.io.base import IO
from interloper.partitioning.partition import TimePartition
from interloper.source import Source

logger = logging.getLogger(__name__)


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

    default_asset_args = spec.get("args", {})

    if isinstance(source_or_asset, Asset):
        source_or_asset.bind(**default_asset_args)
    else:
        for asset in source_or_asset.assets:
            asset.bind(**default_asset_args)

    return source_or_asset


def _load_single_io_from_spec(spec: dict) -> IO:
    io_type = _import_io_type_from_path(spec["path"])
    kwargs = spec.get("config", {})
    return io_type(**kwargs)


def _load_io_from_spec(spec: dict) -> dict[str, IO]:
    return {io_name: _load_single_io_from_spec(io_spec) for io_name, io_spec in spec.items()}


def run(
    file: str,
    from_config: bool = False,
    partition: str | None = None,
) -> None:
    assets: set[Source | Asset] = set()
    time_partition = TimePartition(dt.date.fromisoformat(partition)) if partition else None

    if from_config:
        with open(file) as f:
            config = yaml.safe_load(f)

        io = _load_io_from_spec(config["io"]) if "io" in config else {}

        pipeline_spec = config["pipeline"]
        for asset_spec in pipeline_spec:
            source_or_asset = _load_asset_or_source_from_spec(asset_spec)

            if len(source_or_asset.io) == 0:
                source_or_asset.io = io

            assets.add(source_or_asset)

    pipeline = Pipeline(list(assets))
    pipeline.materialize(partition=time_partition)


def main() -> None:
    # logging.basicConfig(level=logging.WARNING)

    parser = argparse.ArgumentParser(description="Interloper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("file", type=str, help="Path to script, or config if --from-config is provided")
    run_parser.add_argument("--from-config", action="store_true", help="Load configuration from file")
    run_parser.add_argument("--partition", type=str, help="Partition to materialize (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.command == "run":
        run(args.file, args.from_config, args.partition)


if __name__ == "__main__":
    main()
