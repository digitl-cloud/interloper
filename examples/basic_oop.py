from collections.abc import Sequence
from typing import Any

from interloper.core.asset import Asset
from interloper.core.io import FileIO
from interloper.core.pipeline import Pipeline
from interloper.core.source import Source


class MyAssetA(Asset):
    def data(self) -> Any:
        return "A"


class MyAssetB(Asset):
    def data(
        self,
    ) -> Any:
        return "B"


class MySource(Source):
    def asset_definitions(self) -> Sequence[Asset]:
        return (
            MyAssetA("A"),
            MyAssetB("B"),
        )


x = MySource(
    name="my_source",
    io={"file": FileIO("data")},
)
Pipeline(x).materialize()
