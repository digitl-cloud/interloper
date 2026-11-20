"""Monthly demo source: one standalone asset partitioned by month."""

from __future__ import annotations

from typing import TYPE_CHECKING

import interloper as il

from interloper_assets.demo.basic.source import DemoSchema

if TYPE_CHECKING:
    import pandas as pd


@il.source(tags=["Testing"], icon="carbon:calendar")
class DemoMonthlySource(il.Source):
    """Monthly demo source: one standalone asset partitioned by month."""

    @il.asset(
        schema=DemoSchema,
        partitioning=il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.MONTH),
        tags=["Report"],
    )
    def monthly(self, context: il.ExecutionContext) -> pd.DataFrame:
        """One row per monthly partition, stamped with the period start."""
        import pandas as pd

        return pd.DataFrame([{"date": context.partition.value, "hello": "monthly"}])
