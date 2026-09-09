"""Destination: the IO component for reading and writing asset data."""

from __future__ import annotations

from typing import Any, ClassVar

from interloper.component import Component, ComponentDefinition
from interloper.destination.context import IOContext
from interloper.partitioning.base import Partition
from interloper.utils.text import to_label


class DestinationDefinition(ComponentDefinition):
    """Definition of a destination with its config schema inlined.

    Cross-entity references use keys: ``relations`` names the kinds and keys
    that may fill each declared link. Same-entity data is inlined:
    ``config_schema`` is the destination's own JSON Schema.
    """


class Destination(Component):
    """A component that reads and writes asset data, one partition at a time.

    A destination stores data per **partition**, ``None`` standing for the
    whole of an unpartitioned asset. Subclass and implement
    :meth:`write_partition` and :meth:`read_partition` for a single one;
    :meth:`write` and :meth:`read` own the rest, splitting a window write into
    one call per partition and gathering a window read into one result per
    partition, so a destination is partition-correct by construction. A
    backend that does not store per partition (a database that clears each
    partition and inserts a window in one batch) overrides :meth:`write` or
    :meth:`read` instead.

    The hooks may be plain sync methods (the common case: most warehouse and
    file clients are sync) or ``async def`` for native async I/O. The engine
    is async-native: it awaits async implementations directly and offloads
    sync ones to a worker thread, so a destination never blocks the event
    loop either way. An annotation naming a component class declares a
    relation, which the destination resolves by name::

        class JSONDestination(Destination):
            connection: BucketConnection

            def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
                self.connection.put(self._path(context, partition), json.dumps(data, default=str))

            def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
                return json.loads(self.connection.get(self._path(context, partition)))
    """

    tags: ClassVar[list[str]] = []

    @classmethod
    def definition(cls) -> DestinationDefinition:
        """Produce a structured definition of this destination class.

        The config schema is inlined; relations reference their targets by key.

        Returns:
            A DestinationDefinition with metadata and JSON Schema.
        """
        from interloper.resource.fields import validate_fetch_field_providers
        from interloper.utils.imports import get_object_path

        validate_fetch_field_providers(cls, cls.relations)

        return DestinationDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(cls.tags),
            config_schema=cls.config_schema(),
            relations=dict(cls.relations),
        )

    def write_partition(self, context: IOContext, partition: Partition | None, data: Any) -> None:
        """Store *data* for one partition.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition being stored, or ``None`` for the
                unpartitioned whole.
            data: The partition's slice of the data to store.

        Raises:
            NotImplementedError: Every destination implements this, unless it
                overrides :meth:`write` for storage that is not per partition.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement write_partition()")

    def read_partition(self, context: IOContext, partition: Partition | None) -> Any:
        """Load one partition.

        Args:
            context: IO context carrying the target asset and the effective schema.
            partition: The partition to load, or ``None`` for the unpartitioned
                whole.

        Raises:
            NotImplementedError: Every destination implements this, unless it
                overrides :meth:`read` for storage that is not per partition.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement read_partition()")

    def write(self, context: IOContext, data: Any) -> None:
        """Write data, one partition at a time.

        A window is split into one :meth:`write_partition` call per partition,
        each receiving its slice of the data; a single partition or the
        unpartitioned whole is one call receiving the data as is.

        Args:
            context: IO context carrying the target asset, the partition or window,
                and the effective schema.
            data: The data to write, in its native representation.
        """
        for partition, chunk in context.slices(data):
            self.write_partition(context, partition, chunk)

    def read(self, context: IOContext) -> Any:
        """Read data for the context's partition, or window.

        Args:
            context: IO context carrying the target asset, the partition or window,
                and the effective schema.

        Returns:
            The partition's data; a window returns one result per partition, in
            window order.
        """
        results = [self.read_partition(context, partition) for partition in context.partitions]
        return results if context.window else results[0]

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        The partition column is read from ``context.asset.partitioning.column``.
        Each key in the returned dict is the string representation of a partition
        value; each value is the number of rows in that partition.

        Args:
            context: Destination context (uses ``asset.table``, ``asset.dataset``,
                and ``asset.partitioning``).

        Returns:
            Mapping from partition value (as string) to row count.
        """
        raise NotImplementedError("partition_row_counts is not implemented")
