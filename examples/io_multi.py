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
    @asset
    def MyAssetA() -> str:
        return "A"

    @asset
    def MyAssetB(
        a: str = UpstreamAsset("A"),
    ) -> str:
        return "B"

    return (MyAssetA, MyAssetB)


MySource.io = {
    "file": FileIO("data"),
    "file2": FileIO("data2"),
}
MySource.default_io_key = "file"

Pipeline(MySource).materialize()
