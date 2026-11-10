"""Monthly demo source: one standalone asset partitioned by month."""

import interloper as il
import pandas as pd

from interloper_assets.demo.basic.source import DemoSchema


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
        return pd.DataFrame([{"date": context.partition.value, "hello": "monthly"}])
