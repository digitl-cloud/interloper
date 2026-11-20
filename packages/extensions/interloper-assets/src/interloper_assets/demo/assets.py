"""Standalone demo asset, outside any source."""

from __future__ import annotations

from typing import TYPE_CHECKING

import interloper as il

from interloper_assets.demo.basic.source import DemoSchema, partitioning

if TYPE_CHECKING:
    import pandas as pd


@il.asset(
    schema=DemoSchema,
    partitioning=partitioning,
    tags=["Report"],
)
def demo_asset(context: il.ExecutionContext) -> pd.DataFrame:
    """Demo asset. Returns a single row with the configured greeting."""
    import pandas as pd

    return pd.DataFrame([{"date": context.partition_date, "hello": "world"}])
