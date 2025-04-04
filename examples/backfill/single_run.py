import datetime as dt
import logging
from collections.abc import Sequence

import interloper as itlp

itlp.basic_logging(logging.INFO)


@itlp.source
def my_source() -> Sequence[itlp.Asset]:
    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date", allow_window=True),
    )
    def my_asset(
        date_window: tuple[dt.date, dt.date] = itlp.DateWindow(),
    ) -> str:
        return "hello"

    return (my_asset,)


my_source.io = {"file": itlp.FileIO("data")}

itlp.Pipeline(my_source).backfill(
    partitions=itlp.TimePartitionRange(
        start=dt.date.today() - dt.timedelta(days=3),
        end=dt.date.today() - dt.timedelta(days=1),
    )
)
