from dataclasses import dataclass
from typing import TYPE_CHECKING

from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow

if TYPE_CHECKING:
    from interloper.asset import Asset


@dataclass(frozen=True)
class ExecutionContext:
    assets: dict[str, "Asset"]
    executed_asset: "Asset"
    partition: Partition | PartitionWindow | None = None
