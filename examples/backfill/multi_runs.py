import datetime as dt
import logging
from collections.abc import Sequence

import interloper as itlp

itlp.basic_logging(logging.INFO)


@itlp.source
def my_source() -> Sequence[itlp.Asset]:
    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date"),
    )
    def my_asset(
        date: dt.date = itlp.Date(),
    ) -> str:
        return "hello"

    return (my_asset,)


my_source.io = {"file": itlp.FileIO("data")}

itlp.DAG(my_source).backfill(
    partitions=itlp.TimePartitionWindow(
        start=dt.date.today() - dt.timedelta(days=3),
        end=dt.date.today() - dt.timedelta(days=1),
    )
)
