import logging
from collections.abc import Sequence

import interloper as itlp

itlp.basic_logging(logging.INFO)


@itlp.source
def my_source() -> Sequence[itlp.Asset]:
    @itlp.asset(name="A")
    def my_asset_A() -> str:
        return "A"

    @itlp.asset(name="B")
    def my_asset_B(
        a: str = itlp.UpstreamAsset("A"),
    ) -> str:
        return "B"

    @itlp.asset(name="C")
    def my_assetC(
        a: str = itlp.UpstreamAsset("custom_ref_A"),
        b: str = itlp.UpstreamAsset("custom_ref_B"),
    ) -> str:
        return "C"

    return (my_asset_A, my_asset_B, my_assetC)


my_source.io = {"file": itlp.FileIO("data")}

# Upstream assets's refs do not match the name of the corresponding assets
# therefore, the source cannot build the deps config map automatically and it has to be defined manually
my_source.C.deps = {
    "custom_ref_A": "A",
    "custom_ref_B": "B",
}

itlp.Pipeline(my_source).materialize()
