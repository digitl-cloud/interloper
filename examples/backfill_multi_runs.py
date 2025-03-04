import datetime as dt
import logging
from collections.abc import Sequence

from interloper.core.asset import Asset, asset
from interloper.core.io import FileIO
from interloper.core.param import Date
from interloper.core.partitioning import TimePartitionRange, TimePartitionStrategy
from interloper.core.pipeline import Pipeline
from interloper.core.source import source
from interloper.core.utils import basic_logging

basic_logging(logging.INFO)


@source
def my_source() -> Sequence[Asset]:
    @asset(
        partition_strategy=TimePartitionStrategy(column="date"),
    )
    def my_asset(
        date: dt.date = Date(),
    ) -> str:
        return "hello"

    return (my_asset,)


my_source.io = {"file": FileIO("data")}

Pipeline(my_source).backfill(
    partitions=TimePartitionRange(
        start=dt.date.today() - dt.timedelta(days=3),
        end=dt.date.today() - dt.timedelta(days=1),
    )
)
