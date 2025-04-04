import datetime as dt
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Partition:
    value: Any

    @property
    def id(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class TimePartition(Partition):
    value: dt.date

    def __repr__(self):
        return self.value.isoformat()

    @property
    def id(self) -> str:
        return self.value.strftime("%Y%m%d")
