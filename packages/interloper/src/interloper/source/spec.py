"""This module contains the source specification."""

from typing import Any, Literal

from pydantic import BaseModel

from interloper.io.spec import IOSpec
from interloper.source.base import Source
from interloper.utils.loader import import_from_path


class SourceSpec(BaseModel):
    """A specification for a source.

    Attributes:
        name: The name of the source.
        path: The path to the source.
        io: The IOs for the source.
        assets_args: The arguments for the assets in the source.
    """

    type: Literal["source"]
    name: str
    path: str
    io: dict[str, IOSpec] | None = None
    assets: list[str] | None = None
    assets_args: dict[str, Any] | None = None

    def to_source(self) -> Source:
        """Create a source from the specification.

        Raises:
            ValueError: If the asset name in `assets` is not a valid asset of the source.

        Returns:
            A source.
        """
        source: Source = import_from_path(self.path)
        source.name = self.name

        if self.io:
            source.io = {name: spec.to_io() for name, spec in self.io.items()}

        # If assets are specified, all assets are "disabled" by making them non-materializable.
        # Then, the assets specified in `assets` are made materializable.
        if self.assets:
            source.materializable = False
            for asset_name in self.assets:
                if asset_name not in source:
                    raise ValueError(f"Asset {asset_name} is not a valid asset of source {self.name}")
                source[asset_name].materializable = True

        if self.assets_args:
            source.default_assets_args = self.assets_args

        return source
