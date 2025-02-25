import datetime as dt
from dataclasses import dataclass


class Partition: ...


@dataclass(frozen=True)
class TimePartition(Partition):
    date: dt.date
