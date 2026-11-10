"""Standalone demo asset, outside any source."""

import interloper as il
import pandas as pd

from interloper_assets.demo.basic.source import DemoSchema, partitioning


@il.asset(
    schema=DemoSchema,
    partitioning=partitioning,
    tags=["Report"],
)
def demo_asset(context: il.ExecutionContext) -> pd.DataFrame:
    """Demo asset. Returns a single row with the configured greeting."""
    return pd.DataFrame([{"date": context.partition_date, "hello": "world"}])
