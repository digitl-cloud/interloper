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
            when the upstream has nothing materialized where the destination
            looks (no table or object for that scope at all); a ``LOG``
            warning names the leg. An existing but empty scope is not this
            case: it arrives as whatever the destination returns for an
            empty read (an empty list or frame), not ``None``.
    """

    asset: Asset
    data: Any
