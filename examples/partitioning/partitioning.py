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
    def my_asset_A(
        date: dt.date = itlp.Date(),
    ) -> str:
        return "A"

    @itlp.asset(
        partitioning=itlp.TimePartitionConfig(column="date", allow_window=True),
    )
    def my_asset_B(
        date_window: tuple[dt.date, dt.date] = itlp.DateWindow(),
    ) -> str:
        return "B"

    return (my_asset_A,)


my_source.io = {"file": itlp.FileIO("data")}

itlp.Pipeline(my_source).materialize(partition=itlp.TimePartition(dt.date.today()))
