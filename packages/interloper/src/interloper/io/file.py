import logging
import pickle
from pathlib import Path
from typing import Any

from interloper.io.base import IO, IOContext
from interloper.partitioning.partitions import Partition
from interloper.partitioning.ranges import PartitionRange

logger = logging.getLogger(__name__)


class FileIO(IO):
    def __init__(self, base_dir: str) -> None:
        self.folder = base_dir

    def write(self, context: IOContext, data: Any) -> None:
        if context.partition:
            if isinstance(context.partition, PartitionRange):
                raise RuntimeError("Partition ranges are not supported by FileIO")
            else:
                self._write_partition(context.asset.name, context.partition, data)
                logger.info(
                    f"Asset {context.asset.name} partition written to "
                    f"{self.folder}/{context.asset.name}${context.partition.id}"
                )
        else:
            self._write_data(self.folder, context.asset.name, data)
            logger.info(f"Asset {context.asset.name} written to {self.folder}/{context.asset.name}")

    def read(self, context: IOContext) -> Any:
        if context.partition:
            if isinstance(context.partition, PartitionRange):
                raise RuntimeError("Partition ranges are not supported by FileIO")
            else:
                data = self._read_partition(context.asset.name, context.partition)
                logger.info(
                    f"Asset {context.asset.name} partition read from "
                    f"{self.folder}/{context.asset.name}${context.partition.id}"
                )
        else:
            data = self._read_data(self.folder, context.asset.name)
            logger.info(f"Asset {context.asset.name} read from {self.folder}/{context.asset.name}")
        return data

    def _write_data(self, folder: str, asset_name: str, data: Any) -> None:
        with open(Path(folder) / asset_name, "wb") as f:
            f.write(pickle.dumps(data))

    def _write_partition(self, asset_name: str, partition: Partition, data: Any) -> None:
        self._write_data(self.folder, f"{asset_name}${partition.id}", data)

    def _read_data(self, folder: str, asset_name: str) -> Any:
        with open(Path(folder) / asset_name, "rb") as f:
            data = pickle.loads(f.read())
        return data

    def _read_partition(self, asset_name: str, partition: Partition) -> Any:
        return self._read_data(self.folder, f"{asset_name}${partition.id}")
