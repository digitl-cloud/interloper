"""Example demonstrating the Source component with the @source decorator."""

from __future__ import annotations

import asyncio
from typing import Any

import interloper as il


class SimpleSchema(il.Schema):
    id: int
    name: str


class SimpleDestination(il.Destination):
    table: str = "default"

    def read(self, context: il.IOContext) -> str:
        """Read data from this destination."""
        return f"read from {self.table}"

    def write(self, context: il.IOContext, data: Any) -> None:
        """Write data to this destination."""
        print(f"  [{self.table}] wrote: {data}")


@il.source(
    destinations=[SimpleDestination],
)
def SimpleSource() -> list[type[il.Asset]]:
    @il.asset(
        schema=SimpleSchema,
    )
    def users() -> list[dict[str, Any]]:
        return [{"id": 1, "name": "Alice"}]

    @il.asset
    def orders() -> list[dict[str, Any]]:
        return [{"id": 100, "item": "Widget"}]

    @il.asset
    def metrics() -> dict[str, float]:
        return {"uptime": 99.9, "latency_ms": 42.0}

    return [users, orders, metrics]


if __name__ == "__main__":
    s = SimpleSource()

    print("=== Source ===")
    print(f"key: {s.key}")
    print(f"assets: {[a.key for a in s.assets]}")

    print("\n=== Asset lookup ===")
    print(f"s.users.key: {s.users.key}")
    print(f"s.orders.key: {s.orders.key}")
    print(f"s.metrics.key: {s.metrics.key}")

    print("\n=== Run ===")
    for a in s.assets:
        print(f"{a.key}: {asyncio.run(a.run())}")

    print("\n=== Reconfigure ===")
    disabled = s(materializable=False)
    for a in disabled.assets:
        print(f"{a.key}: materializable={a.materializable}")

    print("\n=== Original unchanged ===")
    for a in s.assets:
        print(f"{a.key}: materializable={a.materializable}")
