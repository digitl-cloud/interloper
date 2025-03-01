import datetime as dt
import logging
from collections.abc import Sequence

from interloper.core.asset import Asset, asset
from interloper.core.io import FileIO
from interloper.core.param import DateWindow
from interloper.core.partitioning import TimePartitionRange, TimePartitionStrategy
from interloper.core.pipeline import Pipeline
from interloper.core.source import source
from interloper.core.utils import basic_logging

basic_logging(logging.INFO)


@source
def MySource() -> Sequence[Asset]:
    @asset(
        partition_strategy=TimePartitionStrategy(column="date", allow_window=True),
    )
    def MyAsset(
        date_window: tuple[dt.date, dt.date] = DateWindow(),
    ) -> str:
        return "hello"

    return (MyAsset,)


MySource.io = {"file": FileIO("data")}

Pipeline(MySource).backfill(
    partitions=TimePartitionRange(
        start=dt.date.today() - dt.timedelta(days=3),
        end=dt.date.today() - dt.timedelta(days=1),
    )
)
