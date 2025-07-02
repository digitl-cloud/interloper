"""This module contains the AssetSpec class."""

from typing import Any, Literal

from pydantic import BaseModel

from interloper.asset.base import Asset
from interloper.io.spec import IOSpec
from interloper.utils.loader import import_from_path


class AssetSpec(BaseModel):
    """A specification for an asset.

    Attributes:
        name: The name of the asset.
        path: The path to the asset class.
        io: The IO specifications for the asset.
        args: The arguments for the asset.
    """

    type: Literal["asset"]
    name: str
    path: str
    io: dict[str, IOSpec] = {}
    args: dict[str, Any] | None = None

    def to_asset(self) -> Asset:
        """Create an asset from the specification.

        Returns:
            An asset.
        """
        AssetType: type[Asset] = import_from_path(self.path)

        print(AssetType)

        asset = AssetType(
            name=self.name,
            io={name: spec.to_io() for name, spec in self.io.items()},
        )

        if self.args:
            asset.bind(**self.args)

        return asset
