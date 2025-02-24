from collections.abc import Sequence
from typing import Any

from dead.core.asset import Asset, asset
from dead.core.io import FileIO
from dead.core.pipeline import Pipeline
from dead.core.param import Env, UpstreamAsset
from dead.core.source import source


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
    "file": FileIO("/Users/g/Downloads/dead"),
    "file2": FileIO("/Users/g/Downloads/dead2"),
}
X.default_io_key = "file"

pipeline = Pipeline(X)
pipeline.materialize()
