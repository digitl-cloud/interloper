"""Simple end-to-end example of the interloper component system."""

from __future__ import annotations

from typing import Any

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
    store: list[Any] = []

    def read(self, context: il.IOContext) -> list[Any]:
        """Read data from this destination."""
        return self.store

    def write(self, context: il.IOContext, data: Any) -> None:
        """Write data to this destination."""
        self.store.append(data)


@il.asset(
    partitioning=il.TimePartitionConfig(column="date"),
    schema=SimpleSchema,
    destinations=[SimpleDestination],
)
def simple_asset(
    context: il.ExecutionContext,
    connection: SimpleConnection,
) -> list[dict[str, Any]]:
    return [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
