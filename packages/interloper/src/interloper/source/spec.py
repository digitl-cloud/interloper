from dataclasses import dataclass
from typing import Any

from interloper.io.base import IO
from interloper.io.specs import IOSpec
from interloper.source.base import Source
from interloper.utils.loader import import_from_path


@dataclass(frozen=True)
class SourceSpec:
    name: str
    path: str
    io: dict[str, IOSpec] | None = None
    assets_args: dict[str, Any] | None = None

    def to_source(self) -> Source:
        source: Source = import_from_path(self.path)

        io: dict[str, IO] = {}
        if self.io:
            io = {name: spec.to_io() for name, spec in self.io.items()}
        source.io = io

        if self.assets_args:
            source.bind(**self.assets_args, ignore_unknown_params=True)

        return source
