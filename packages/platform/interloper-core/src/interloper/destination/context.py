"""Frozen context object passed to every IO read/write call."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from interloper.errors import ConfigError
from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.asset.base import Asset
    from interloper.schema import Schema


@dataclass(frozen=True)
class IOContext:
    """Immutable context passed to `Destination.read` and `Destination.write`.

    Carries the target asset, the window the call covers and the run metadata, so a destination
    resolves the storage location, plus the *effective* schema of the data: the asset's declared
    schema when set, otherwise the one inferred during conform, ``None`` when none could be
    resolved. Destinations use it for DDL, typed loads and restoring types on read.

    A destination stores data per **partition**, ``None`` standing for the unpartitioned whole;
    `partitions` and `slices` spell that out, so no destination branches on the window itself.
    """

    asset: Asset
    window: PartitionWindow | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema: type[Schema] | None = None

    @property
    def partitions(self) -> list[Partition | None]:
        """The partitions this context covers, first to last.

        Returns:
            ``[None]`` for the unpartitioned whole, else the window's partitions.
        """
        return [None] if self.window is None else list(self.window.partitions)

    def slices(self, data: Any) -> list[tuple[Partition | None, Any]]:
        """Pair each partition with its slice of *data*.

        A window of several partitions is split on the asset's partition column through the
        data's representation; a single partition, or the whole, receives the data as is.

        Args:
            data: The data being written, in its native representation.

        Returns:
            ``(partition, slice)`` pairs, one per partition, in `partitions` order.

        Raises:
            ConfigError: If a window of several partitions is written for an asset that declares
                no partitioning, since nothing says which column to slice on.
        """
        partitions = self.partitions
        if len(partitions) == 1:
            return [(partitions[0], data)]
        if self.asset.partitioning is None:
            raise ConfigError(f"Asset '{self.asset.key}' is not partitioned and cannot be written over a window")
        column = self.asset.partitioning.column
        return [(partition, partition.slice(data, column)) for partition in partitions if partition is not None]
