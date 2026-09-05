"""The leg object handed to ``data()`` for many-valued upstream slots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interloper.asset.base import Asset


@dataclass(frozen=True)
class Upstream:
    """One leg of a many-valued slot as handed to ``data()``.

    Attributes:
        asset: The upstream asset the data was read from; its ``source``,
            ``identity`` and ``id`` tell the legs apart.
        data: The data read from the upstream's destination for the run's
            partition, in that destination's read representation. ``None``
            when the upstream holds no data for that partition; a ``LOG``
            warning names the leg.
    """

    asset: Asset
    data: Any
