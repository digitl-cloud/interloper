import datetime as dt
import logging
from collections.abc import Sequence

from interloper.core.asset import Asset, asset
from interloper.core.io import FileIO
from interloper.core.param import Date, DateWindow
from interloper.core.partitioning import TimePartition, TimePartitionStrategy
from interloper.core.pipeline import Pipeline
from interloper.core.source import source
from interloper.core.utils import basic_logging

basic_logging(logging.INFO)


@source
def MySource() -> Sequence[Asset]:
    @asset(
        partition_strategy=TimePartitionStrategy(column="date"),
    )
    def MyAssetA(
        date: dt.date = Date(),
    ) -> str:
        return "A"

    @asset(
        partition_strategy=TimePartitionStrategy(column="date", allow_window=True),
    )
    def MyAssetB(
        date_window: tuple[dt.date, dt.date] = DateWindow(),
    ) -> str:
        return "B"

    return (MyAssetA,)


MySource.io = {"file": FileIO("data")}

Pipeline(MySource).materialize(partition=TimePartition(dt.date.today()))
