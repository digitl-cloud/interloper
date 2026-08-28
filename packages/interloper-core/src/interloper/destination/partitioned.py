"""Partition-aware destination template: the three-way scope dispatch, once."""

from __future__ import annotations

from typing import Any

from interloper.destination.base import Destination
from interloper.destination.context import IOContext
from interloper.partitioning.base import Partition, PartitionWindow


class PartitionedDestination(Destination):
    """Destination base implementing the partition dispatch once.

    The ``None / Partition / PartitionWindow`` branching used to be
    hand-rolled by every destination — the source of the window-duplication
    bug where each partition directory received the *full* dataset.
    Subclasses implement two scope hooks and are partition-correct by
    construction:

    - :meth:`_write_scope` — store data for one scope (``partition=None``
      means the unpartitioned whole).
    - :meth:`_read_scope` — load one scope.

    Window writes are split per partition through the data's registered
    representation; window reads return one result per partition. Data whose
    representation cannot be recognized is passed to the write hook as-is
    (it cannot be split).

    Backends whose scoping semantics differ from per-scope storage may
    override :meth:`write` or :meth:`read` wholesale instead of implementing
    the corresponding hook — :class:`DatabaseDestination` does this for
    writes (scoped deletes followed by a single insert).
    """

    def _write_scope(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Store *data* for a single scope.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the
                unpartitioned whole.
            data: The scope's slice of the data to store.

        Raises:
            NotImplementedError: Subclasses implement this or override ``write``.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement _write_scope() or override write().")

    def _read_scope(self, context: IOContext, partition: Partition | None) -> Any:
        """Load a single scope.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition to load, or ``None`` for the unpartitioned
                whole.

        Raises:
            NotImplementedError: Subclasses implement this or override ``read``.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement _read_scope() or override read().")

    def write(self, context: IOContext, data: Any) -> None:
        """Write data, splitting partition windows per partition.

        Args:
            context: IO context carrying the target asset, the partition scope,
                and the effective schema.
            data: The data to write, in its native representation.
        """
        scope = context.partition_or_window
        if scope is None:
            self._write_scope(context, None, data)
        elif isinstance(scope, PartitionWindow):
            assert context.asset.partitioning
            column = context.asset.partitioning.column
            for partition in scope:
                self._write_scope(context, partition, partition.slice(data, column))
        else:
            assert isinstance(scope, Partition)
            self._write_scope(context, scope, data)

    def read(self, context: IOContext) -> Any:
        """Read data for the context's scope.

        Args:
            context: IO context carrying the target asset, the partition scope,
                and the effective schema.

        Returns:
            The scope's data; partition windows return one result per
            partition, in window order.
        """
        scope = context.partition_or_window
        if scope is None:
            return self._read_scope(context, None)
        if isinstance(scope, PartitionWindow):
            return [self._read_scope(context, partition) for partition in scope]
        assert isinstance(scope, Partition)
        return self._read_scope(context, scope)
