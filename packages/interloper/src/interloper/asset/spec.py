from dataclasses import dataclass
from typing import Any

from interloper.asset.base import Asset
from interloper.io.base import IO
from interloper.io.specs import IOSpec
from interloper.utils.loader import import_from_path


@dataclass(frozen=True)
class AssetSpec:
    name: str
    path: str
    io: dict[str, IOSpec]
    args: dict[str, Any] | None = None

    def to_asset(self) -> Asset:
        AssetType: type[Asset] = import_from_path(self.path)
        io: dict[str, IO] = {name: spec.to_io() for name, spec in self.io.items()}
        return AssetType(
            name=self.name,
            io=io,
        )
