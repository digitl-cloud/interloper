"""Simple end-to-end example of the interloper component system."""

from __future__ import annotations

from typing import Any, ClassVar

import interloper as il


@il.schema
class SimpleSchema:
    id: int
    name: str
    email: str


@il.connection
class SimpleConnection:
    host: str = "localhost"
    port: int = 5432
    username: str = "postgres"
    password: str = "postgres"


@il.destination
class SimpleDestination:
    table: str = "users"
    store: ClassVar[dict[str, Any]] = {}

    def write_partition(self, context: il.IOContext, partition: il.Partition | None, data: Any) -> None:
        """Keep one partition's data in memory."""
        self.store[partition.id if partition else "all"] = data

    def read_partition(self, context: il.IOContext, partition: il.Partition | None) -> Any:
        """Return one partition's data."""
        return self.store[partition.id if partition else "all"]

    def partition_row_counts(self, context: il.IOContext) -> dict[str, int]:
        """Count the rows held per partition."""
        return {key: len(rows) for key, rows in self.store.items()}


@il.asset(
    partitioning=il.TimePartitionConfig(column="date"),
    schema=SimpleSchema,
    relations={"destinations": [SimpleDestination]},
)
def simple_asset(
    context: il.ExecutionContext,
    connection: SimpleConnection,
) -> list[dict[str, Any]]:
    return [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
