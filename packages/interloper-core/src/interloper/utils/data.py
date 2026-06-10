"""Data utility helpers."""

from __future__ import annotations

import types
from collections.abc import Generator, Iterator
from typing import Any

from pydantic import BaseModel

from interloper.errors import NormalizerError


def coerce_to_records(data: Any) -> list[dict[str, Any]]:
    """Coerce tabular-ish data to ``list[dict]`` records.

    Supported types: ``dict``, ``list[dict]``, ``BaseModel``,
    ``list[BaseModel]``, ``Generator`` / ``Iterator``, ``None``.

    Returns:
        The coerced list of row dicts.

    Raises:
        NormalizerError: If the data type is unsupported.
    """
    if data is None:
        return []

    # Generator / Iterator -> consume then re-process
    if isinstance(data, (Generator, Iterator, types.GeneratorType)):
        return coerce_to_records(list(data))

    # Single Pydantic model
    if isinstance(data, BaseModel):
        return [data.model_dump()]

    # list
    if isinstance(data, list):
        if not data:
            return []
        first = data[0]
        if isinstance(first, dict):
            return data
        if isinstance(first, BaseModel):
            return [item.model_dump() for item in data]
        raise NormalizerError(
            f"Normalizer received list[{type(first).__name__}], expected list[dict] or list[BaseModel]."
        )

    # Single dict
    if isinstance(data, dict):
        return [data]

    raise NormalizerError(
        f"Normalizer does not support type {type(data).__name__}. "
        "Supported: dict, list[dict], BaseModel, list[BaseModel], Generator."
    )


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
