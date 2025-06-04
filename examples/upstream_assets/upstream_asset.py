import logging
from collections.abc import Sequence

import interloper as itlp

itlp.basic_logging(logging.DEBUG)


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
    def my_asset_C(
        a: str = itlp.UpstreamAsset("custom_ref_A"),
        b: str = itlp.UpstreamAsset("custom_ref_B"),
    ) -> str:
        return "C"

    return (my_asset_A, my_asset_B, my_asset_C)


my_source.io = {"file": itlp.FileIO("data")}

# Upstream assets's keys do not match the name of the corresponding assets therefore, the source cannot build the deps
# config map automatically and it has to be defined manually. Note that the ref then points to the asset ID (+source).
my_source.C.deps = {
    "custom_ref_A": "my_source.A",
    "custom_ref_B": "my_source.B",
}

itlp.DAG(my_source).materialize()
