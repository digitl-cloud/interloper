from collections.abc import Sequence
from typing import Any

from interloper.core.asset import Asset
from interloper.core.io import FileIO
from interloper.core.pipeline import Pipeline
from interloper.core.param import UpstreamAsset
from interloper.core.source import Source


class AssetA(Asset):
    def data(self) -> Any:
        return "A"


class AssetB(Asset):
    def data(
        self,
        a: Any = UpstreamAsset("A"),
    ) -> Any:
        return "B"


class SourceX(Source):
    def asset_definitions(self) -> Sequence[Asset]:
        return (
            AssetA("A"),
            AssetB("B"),
        )


z = SourceX(
    name="X",
    io={"file": FileIO("/Users/g/Downloads/interloper")},
)
Pipeline(z).materialize()
