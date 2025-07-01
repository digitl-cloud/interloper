"""This module contains the AssetSpec class."""
from dataclasses import dataclass
from typing import Any

from interloper.asset.base import Asset
from interloper.io.base import IO
from interloper.io.specs import IOSpec
from interloper.utils.loader import import_from_path


@dataclass(frozen=True)
class AssetSpec:
    """A specification for an asset.

    Attributes:
        name: The name of the asset.
        path: The path to the asset class.
        io: The IO specifications for the asset.
        args: The arguments for the asset.
    """

    name: str
    path: str
    io: dict[str, IOSpec]
    args: dict[str, Any] | None = None

    def to_asset(self) -> Asset:
        """Create an asset from the specification.

        Returns:
            An asset.
        """
        AssetType: type[Asset] = import_from_path(self.path)
        io: dict[str, IO] = {name: spec.to_io() for name, spec in self.io.items()}
        return AssetType(
            name=self.name,
            io=io,
        )
