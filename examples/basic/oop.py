from collections.abc import Sequence
from typing import Any

import interloper as itlp


class MyAssetA(itlp.Asset):
    def data(self) -> Any:
        return "A"


class MyAssetB(itlp.Asset):
    def data(
        self,
    ) -> Any:
        return "B"


class MySource(itlp.Source):
    def asset_definitions(self) -> Sequence[itlp.Asset]:
        return (
            MyAssetA("my_asset_A"),
            MyAssetB("my_asset_B"),
        )


my_source = MySource("my_source")

my_source.my_asset_A.run()
