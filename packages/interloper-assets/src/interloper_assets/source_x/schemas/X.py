from dataclasses import field

import interloper as itlp


class X(itlp.AssetSchema):
    x: int = field(metadata={"description": "x description"})
