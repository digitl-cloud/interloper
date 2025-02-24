from collections.abc import Sequence
from typing import Any

from dead.core.asset import Asset
from dead.core.io import FileIO
from dead.core.pipeline import Pipeline
from dead.core.param import UpstreamAsset
from dead.core.source import Source


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
    io={"file": FileIO("/Users/g/Downloads/dead")},
)
Pipeline(z).materialize()
