from collections.abc import Sequence
from typing import Any

from dead.asset import Asset, asset
from dead.io import FileIO
from dead.pipeline import Pipeline
from dead.sentinel import Env, UpstreamAsset
from dead.source import source

##############
# OOO
##############
# class AssetX(Asset):
#     def data(self) -> Any:
#         return ["x1", "x2"]


# class AssetY(Asset):
#     def data(
#         self,
#         x: Any = UpstreamAsset("X"),
#     ) -> Any:
#         return ["y1", "y2"]


# class SourceZ(Source):
#     def asset_definitions(self) -> Sequence[Asset]:
#         return AssetX("X"), AssetY("Y")


# z = SourceZ("Z")
# z.io = FileIO("/Users/g/Downloads/dead")
# Pipeline(z).materialize()


##############
# FUNCTIONAL
##############


@source
def Z(
    key: str = Env("KEY_A"),
) -> Sequence[Asset]:
    @asset
    def X() -> Any:
        return ["x1", "x2"]

    @asset
    def Y(
        x: Any = UpstreamAsset("X"),
    ) -> Any:
        return ["y1", "y2"]

    return (X, Y)


Z.io = {
    "file": FileIO("/Users/g/Downloads/dead"),
    "file2": FileIO("/Users/g/Downloads/dead2"),
}
Z.default_io_key = "file"

Pipeline(Z).materialize()
