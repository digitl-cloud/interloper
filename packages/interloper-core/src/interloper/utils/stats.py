"""Statistics over plain samples."""

from __future__ import annotations

import math
from collections.abc import Sequence


def percentile(values: Sequence[float], q: float) -> float | None:
    """The nearest-rank *q*-th percentile of *values*.

    Args:
        values: The sample; order does not matter.
        q: Percentile in ``[0, 100]``.

    Returns:
        The sample value at that rank, or ``None`` for an empty sample.
    """
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]
