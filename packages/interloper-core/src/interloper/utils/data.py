"""Data utility helpers."""

from __future__ import annotations

import sys
from typing import Any


def is_dataframe(data: Any) -> bool:
    """Return whether *data* is a pandas DataFrame, without importing pandas.

    Uses a ``sys.modules`` guard: if a DataFrame instance exists in the
    process, pandas is by definition already imported, so core never needs
    pandas as a dependency to *recognize* one.
    """
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(data, pd.DataFrame)


def dataframe_to_records(data: Any) -> list[dict[str, Any]]:
    """Convert a DataFrame to a null-safe ``list[dict]`` records view.

    Unlike ``DataFrame.to_dict("records")``, missing values (``NaN``, ``NaT``,
    ``pd.NA``) are mapped to ``None`` so the rows are valid against nullable
    schema fields and serialize to JSON ``null``.

    Args:
        data: A pandas DataFrame (caller must have checked :func:`is_dataframe`).

    Returns:
        Rows as a list of dicts with ``None`` for missing values.
    """
    pd = sys.modules["pandas"]
    return data.astype(object).where(pd.notnull(data), None).to_dict("records")


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
