"""Data utility helpers."""

from __future__ import annotations

from typing import Any


def is_empty(data: Any) -> bool:
    """Return whether a value carries no data.

    Kept deliberately conservative: only values we can *positively* confirm
    are empty count as empty, so callers never skip work on a value they don't
    understand. ``None`` is empty; objects exposing a boolean ``empty`` (pandas
    / polars DataFrames and Series — whose own ``bool()`` raises) defer to it;
    sized containers (lists, dicts, ...) are empty when their length is zero.
    Anything else (e.g. a lazy generator we must not consume) is treated as
    non-empty.
    """
    if data is None:
        return True
    empty = getattr(data, "empty", None)
    if isinstance(empty, bool):
        return empty
    try:
        return len(data) == 0
    except TypeError:
        return False
