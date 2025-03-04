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
            MyAssetA("my_asset_A"),
            MyAssetB("my_asset_B"),
        )


my_source = MySource("my_source")

my_source.my_asset_A.run()
