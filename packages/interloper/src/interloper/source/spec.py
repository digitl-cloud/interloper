"""This module contains the source specification."""
from dataclasses import dataclass
from typing import Any

from interloper.io.base import IO
from interloper.io.specs import IOSpec
from interloper.source.base import Source
from interloper.utils.loader import import_from_path


@dataclass(frozen=True)
class SourceSpec:
    """A specification for a source.

    Attributes:
        name: The name of the source.
        path: The path to the source.
        io: The IOs for the source.
        assets_args: The arguments for the assets in the source.
    """

    name: str
    path: str
    io: dict[str, IOSpec] | None = None
    assets_args: dict[str, Any] | None = None

    def to_source(self) -> Source:
        """Create a source from the specification.

        Returns:
            A source.
        """
        source: Source = import_from_path(self.path)

        io: dict[str, IO] = {}
        if self.io:
            io = {name: spec.to_io() for name, spec in self.io.items()}
        source.io = io

        if self.assets_args:
            source.bind(**self.assets_args, ignore_unknown_params=True)

        return source
