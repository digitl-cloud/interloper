"""Frozen context object passed to every IO read/write call."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.asset.base import Asset


@dataclass(frozen=True)
class IOContext:
    """Immutable context passed to :meth:`Destination.read` and :meth:`Destination.write`.

    Carries the target asset, optional partition scope, and arbitrary metadata
    so that destination implementations can resolve the correct storage location.
    """

    asset: Asset
    partition_or_window: Partition | PartitionWindow | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
