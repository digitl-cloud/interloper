from dataclasses import dataclass
from typing import TYPE_CHECKING

from interloper.partitioning.partition import Partition
from interloper.partitioning.range import PartitionRange

if TYPE_CHECKING:
    from interloper.asset import Asset


@dataclass(frozen=True)
class ExecutionContext:
    assets: dict[str, "Asset"]
    executed_asset: "Asset"
    partition: Partition | PartitionRange | None = None
