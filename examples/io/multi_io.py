import logging
from collections.abc import Sequence

import interloper as itlp

itlp.basic_logging(logging.INFO)


@itlp.source
def my_source() -> Sequence[itlp.Asset]:
    @itlp.asset
    def my_asset_A() -> str:
        return "A"

    @itlp.asset
    def my_asset_B(
        a: str = itlp.UpstreamAsset("A"),
    ) -> str:
        return "B"

    return (my_asset_A, my_asset_B)


my_source.io = {
    "file": itlp.FileIO("data"),
    "file2": itlp.FileIO("data2"),
}
my_source.default_io_key = "file"

itlp.DAG(my_source.my_asset_A).materialize()
