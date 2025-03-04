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
def my_source() -> Sequence[Asset]:
    @asset
    def my_asset_A() -> str:
        return "A"

    @asset
    def my_asset_B(
        a: str = UpstreamAsset("A"),
    ) -> str:
        return "B"

    return (my_asset_A, my_asset_B)


my_source.io = {
    "file": FileIO("data"),
    "file2": FileIO("data2"),
}
my_source.default_io_key = "file"

Pipeline(my_source).materialize()
