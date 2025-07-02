"""This module contains the execution context classes."""
from dataclasses import dataclass
from typing import TYPE_CHECKING

from interloper.partitioning.partition import Partition
from interloper.partitioning.window import PartitionWindow

if TYPE_CHECKING:
    from interloper.asset.base import Asset


@dataclass(frozen=True, kw_only=True)
class ExecutionContext:
    """The execution context.

    Attributes:
        assets: The assets in the execution.
        partition: The partition of the execution.
    """

    assets: dict[str, "Asset"]
    partition: Partition | PartitionWindow | None = None


@dataclass(frozen=True, kw_only=True)
class AssetExecutionContext(ExecutionContext):
    """The execution context for an asset.

    Attributes:
        executed_asset: The asset being executed.
    """

    executed_asset: "Asset"
