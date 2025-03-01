import logging
from collections.abc import Sequence

from interloper.core.asset import Asset, asset
from interloper.core.io import FileIO
from interloper.core.param import UpstreamAsset
from interloper.core.pipeline import Pipeline
from interloper.core.source import source
from interloper.core.utils import basic_logging

basic_logging(logging.INFO)


@source
def MySource() -> Sequence[Asset]:
    @asset(name="A")
    def MyAssetA() -> str:
        return "A"

    @asset(name="B")
    def MyAssetB(
        a: str = UpstreamAsset("A"),
    ) -> str:
        return "B"

    @asset(name="C")
    def MyAssetC(
        a: str = UpstreamAsset("custom_ref_A"),
        b: str = UpstreamAsset("custom_ref_B"),
    ) -> str:
        return "C"

    return (MyAssetA, MyAssetB, MyAssetC)


MySource.io = {"file": FileIO("data")}

# Upstream assets's refs do not match the name of the corresponding assets
# therefore, the source cannot build the deps config map automatically and it has to be defined manually
MySource.C.deps = {
    "custom_ref_A": "A",
    "custom_ref_B": "B",
}

Pipeline(MySource).materialize()
