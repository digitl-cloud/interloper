from dataclasses import dataclass
from typing import Any

from interloper.io.base import IO
from interloper.utils.loader import import_from_path


@dataclass(frozen=True)
class IOSpec:
    path: str
    init: dict[str, Any]

    def to_io(self) -> IO:
        IOType: type[IO] = import_from_path(self.path)
        return IOType(**self.init)