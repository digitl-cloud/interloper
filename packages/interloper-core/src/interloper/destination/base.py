"""Destination: the IO component for reading and writing asset data."""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import Any, ClassVar

from pydantic import Field

from interloper.component import Component, ComponentDefinition
from interloper.destination.context import IOContext
from interloper.resource import Resource
from interloper.utils.text import to_label


class DestinationDefinition(ComponentDefinition):
    """Definition of a destination with its config schema inlined.

    Cross-entity references use keys:
    - ``resource_types`` maps resource name → component key

    Same-entity data is inlined:
    - ``config_schema`` is the destination's own JSON Schema
    """

    tags: list[str] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    resources: dict[str, str] = Field(default_factory=dict)


class Destination(Component):
    """A component that reads and writes asset data.

    Subclass and implement ``read()`` and ``write()`` to define a destination.
    For native async support, override ``aread()`` and ``awrite()`` instead —
    the defaults wrap the sync versions in ``asyncio.to_thread()``::

        class PostgresDestination(Destination):
            resource_types = {"connection": PostgresConnection}

            def read(self, context: IOContext) -> Any:
                conn = self.resources["connection"]
                return query_table(conn.connection_string, context.table)

            def write(self, context: IOContext, data: Any) -> None:
                conn = self.resources["connection"]
                insert_into(conn.connection_string, context.table, data)
    """

    tags: ClassVar[list[str]] = []

    @classmethod
    def definition(cls) -> DestinationDefinition:
        """Produce a structured definition of this destination class.

        The config schema is inlined; resource references use keys.

        Returns:
            A DestinationDefinition with metadata and JSON Schema.
        """
        from interloper.resource.fields import strip_internal_fields, validate_fetch_field_providers
        from interloper.utils.imports import get_object_path

        raw = cls.model_json_schema() if hasattr(cls, "model_json_schema") else {}
        # Resolved resource map (includes annotation-declared slots, not just
        # ``__dict__``) so the DestinationDefinition's ``resources`` and its
        # FetchField pickers work for both declaration styles.
        res_types: dict[str, type[Resource]] = cls.resource_types
        validate_fetch_field_providers(cls, res_types)
        return DestinationDefinition(
            kind=cls.kind,
            key=cls.key,
            path=get_object_path(cls),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(cls.tags),
            config_schema=strip_internal_fields(raw),
            resources={name: res_cls.key for name, res_cls in res_types.items()},
        )

    @abstractmethod
    def read(self, context: IOContext) -> Any:
        """Read data from this destination.

        Args:
            context: Destination context with asset, partition, and metadata.

        Returns:
            The data read from the destination.
        """

    @abstractmethod
    def write(self, context: IOContext, data: Any) -> None:
        """Write data to this destination.

        Args:
            context: Destination context with asset, partition, and metadata.
            data: The data to write.
        """

    async def aread(self, context: IOContext) -> Any:
        """Async read. Defaults to wrapping :meth:`read` in ``asyncio.to_thread``.

        Override this method for native async I/O (e.g. asyncpg, aiofiles).

        Args:
            context: Destination context with asset, partition, and metadata.

        Returns:
            The data read from the destination.
        """
        return await asyncio.to_thread(self.read, context)

    async def awrite(self, context: IOContext, data: Any) -> None:
        """Async write. Defaults to wrapping :meth:`write` in ``asyncio.to_thread``.

        Override this method for native async I/O (e.g. asyncpg, aiofiles).

        Args:
            context: Destination context with asset, partition, and metadata.
            data: The data to write.
        """
        await asyncio.to_thread(self.write, context, data)

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        The partition column is read from ``context.asset.partitioning.column``.
        Each key in the returned dict is the string representation of a partition
        value; each value is the number of rows in that partition.

        Args:
            context: Destination context (uses ``asset.id``, ``asset.dataset``,
                and ``asset.partitioning``).

        Returns:
            Mapping from partition value (as string) to row count.
        """
        raise NotImplementedError("partition_row_counts is not implemented")
