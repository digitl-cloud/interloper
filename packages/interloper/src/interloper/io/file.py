import logging
import pickle
from pathlib import Path
from typing import Any

from opentelemetry import trace

from interloper.asset import Asset
from interloper.io.base import IO, IOContext
from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class FileIO(IO):
    def __init__(self, base_dir: str) -> None:
        self.folder = base_dir

    @tracer.start_as_current_span("interloper.io.FileIO.write")
    def write(self, context: IOContext, data: Any) -> None:
        if context.partition and isinstance(context.partition, PartitionWindow):
            raise RuntimeError("Partition ranges are not supported by FileIO")

        self._write_asset(context.asset, data, context.partition)

    @tracer.start_as_current_span("interloper.io.FileIO.read")
    def read(self, context: IOContext) -> Any:
        if context.partition and isinstance(context.partition, PartitionWindow):
            raise RuntimeError("Partition ranges are not supported by FileIO")

        return self._read_asset(context.asset, context.partition)

    def _write_asset(self, asset: Asset, data: Any, partition: Partition | None = None) -> None:
        if not Path.exists(Path(self.folder)):
            raise FileNotFoundError(f"Folder {self.folder} does not exist")

        path = f"{asset.dataset}/{asset.name}" if asset.dataset else asset.name
        path = f"{self.folder}/{path}"
        path = f"{path}${partition.id}" if partition else path

        Path(path).parent.mkdir(parents=True, exist_ok=True)

        with open(Path(path), "wb") as f:
            f.write(pickle.dumps(data))

        logger.info(f"Asset {asset.name}{f' partition {partition}' if partition else ''} written to {path}")

    def _read_asset(self, asset: Asset, partition: Partition | None = None) -> Any:
        path = f"{asset.dataset}/{asset.name}" if asset.dataset else asset.name
        path = f"{self.folder}/{path}"
        path = f"{path}${partition.id}" if partition else path

        with open(Path(path), "rb") as f:
            data = pickle.loads(f.read())

        logger.info(f"Asset {asset.name}{f' partition {partition}' if partition else ''} read from {path}")

        return data
