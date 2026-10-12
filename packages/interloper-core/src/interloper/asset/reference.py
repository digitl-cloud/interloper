"""Reference: an asset interloper reads but never writes, a table that already exists."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from interloper.asset.base import Asset

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.partitioning.base import Partition, PartitionWindow


class Reference(Asset):
    """An asset whose table exists before interloper does: nothing is created or written.

    A reference points at a table in a destination and knows how to read it.
    ``table_name`` names the table as written, and the asset's ``dataset``
    names where it is, the source's by default; either may name a field of
    the owning source in braces (``"analytics_{property_id}"``), filled by
    `Source.format`. A run does nothing. Downstream assets read it like any
    other asset, and views build on it like any other table.
    """

    reads_upstream_data: ClassVar[bool] = False
    table_name: ClassVar[str | None] = None

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Do nothing: the table exists already.

        Args:
            partition_or_window: Ignored.
            dag: Ignored.
            metadata: Ignored.

        Returns:
            ``None``.
        """
        return None
