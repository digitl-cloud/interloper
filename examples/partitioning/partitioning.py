import datetime as dt
import logging
from collections.abc import Sequence

import interloper as itlp

itlp.basic_logging(logging.INFO)


@itlp.source
def my_source() -> Sequence[itlp.Asset]:
    @itlp.asset(partitioning=itlp.TimePartitionConfig(column="date"))
    def my_asset_A(date: dt.date = itlp.Date()) -> str:
        return "A"

    @itlp.asset(partitioning=itlp.TimePartitionConfig(column="date", allow_window=True))
    def my_asset_B(date_window: tuple[dt.date, dt.date] = itlp.DateWindow()) -> str:
        return "B"

    @itlp.asset(partitioning=itlp.TimePartitionConfig(column="date"))
    def my_asset_C(
        b: str = itlp.UpstreamAsset("my_asset_B"),
        date_window: tuple[dt.date, dt.date] = itlp.DateWindow(),
    ) -> str:
        return "C"

    return (my_asset_A, my_asset_B, my_asset_C)


my_source.io = {"file": itlp.FileIO("data")}

itlp.Pipeline(my_source).materialize(partition=itlp.TimePartition(dt.date.today()))
