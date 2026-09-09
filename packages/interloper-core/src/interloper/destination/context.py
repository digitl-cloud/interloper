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
    """Immutable context passed to :meth:`Destination.read` and :meth:`Destination.write`.

    Carries the target asset, optional partition or window, and arbitrary metadata
    so that destination implementations can resolve the correct storage location.

    ``schema`` is the *effective* schema of the data being written or read:
    the asset's declared schema when set, otherwise the schema inferred during
    conform. Destinations use it for DDL, typed load jobs, and restoring
    types on read. ``None`` when no schema could be resolved.

    A destination stores data per **partition**, ``None`` standing for the
    unpartitioned whole; :attr:`partitions` and :meth:`slices` spell that out
    for the three shapes ``partition_or_window`` can take, so no destination
    branches on them itself.
    """

    asset: Asset
    partition_or_window: Partition | PartitionWindow | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema: type[Schema] | None = None

    @property
    def window(self) -> bool:
        """Whether this context spans several partitions.

        Returns:
            True for a partition window, False for one partition or the
            unpartitioned whole.
        """
        return isinstance(self.partition_or_window, PartitionWindow)

    @property
    def partitions(self) -> list[Partition | None]:
        """The partitions this context covers, in window order.

        Returns:
            ``[None]`` for the unpartitioned whole, ``[partition]`` for one
            partition, one entry per partition for a window.
        """
        target = self.partition_or_window
        if isinstance(target, PartitionWindow):
            return list(target)
        return [target]

    def slices(self, data: Any) -> list[tuple[Partition | None, Any]]:
        """Pair each partition with its slice of *data*.

        A window's data is split on the asset's partition column through the
        data's representation; a single partition, or the whole, receives the
        data as is. Data whose representation is not recognised cannot be
        split and is handed to every partition as is.

        Args:
            data: The data being written, in its native representation.

        Returns:
            ``(partition, slice)`` pairs, one per partition, in :attr:`partitions` order.

        Raises:
            ConfigError: If a window is written for an asset that declares no
                partitioning, since nothing says which column to slice on.
        """
        if not self.window:
            return [(partition, data) for partition in self.partitions]
        if self.asset.partitioning is None:
            raise ConfigError(f"Asset '{self.asset.key}' is not partitioned and cannot be written over a window")
        column = self.asset.partitioning.column
        return [(partition, partition.slice(data, column)) for partition in self.partitions if partition is not None]
