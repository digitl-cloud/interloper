from collections.abc import Sequence
from typing import Any

from interloper.core.asset import Asset, asset
from interloper.core.io import FileIO
from interloper.core.pipeline import Pipeline
from interloper.core.param import Env, UpstreamAsset
from interloper.core.source import source


@source
def X(
    key: str = Env("SUPER_SECRET_KEY"),
) -> Sequence[Asset]:
    @asset
    def A() -> Any:
        return "A"

    @asset
    def B(
        a: Any = UpstreamAsset("A"),
    ) -> Any:
        return "B"

    @asset
    def C(
        a: Any = UpstreamAsset("A"),
        b: Any = UpstreamAsset("B"),
    ) -> Any:
        return "C"

    return (A, B, C)


X.io = {
    "file": FileIO("/Users/g/Downloads/interloper"),
    "file2": FileIO("/Users/g/Downloads/interloper2"),
}
X.default_io_key = "file"

pipeline = Pipeline(X)
pipeline.materialize()
