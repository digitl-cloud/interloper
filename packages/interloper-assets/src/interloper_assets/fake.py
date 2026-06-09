"""Utility for generating fake DataFrames from Schema classes."""

from __future__ import annotations

import datetime as dt
import random
import string
from typing import Any, get_args, get_origin

import pandas as pd
from interloper.schema import Schema


def _fake_value(annotation: Any) -> Any:
    """Generate a single fake value for a given type annotation."""
    origin = get_origin(annotation)
    args = get_args(annotation)

    # Handle Optional / union types (e.g. str | None)
    if origin is type(int | None):  # types.UnionType
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _fake_value(non_none[0])
        return None

    if annotation is str:
        return "".join(random.choices(string.ascii_lowercase, k=8))
    if annotation is int:
        return random.randint(0, 10_000)
    if annotation is float:
        return round(random.uniform(0, 10_000), 2)
    if annotation is bool:
        return random.choice([True, False])
    if annotation is dt.date:
        return dt.date(2025, 1, 1) + dt.timedelta(days=random.randint(0, 365))
    if annotation is dt.datetime:
        return dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=random.randint(0, 365 * 86400))

    return None


def fake_data(
    schema: type[Schema],
    partition_column: str | None = None,
    partition_date: dt.date | None = None,
) -> pd.DataFrame:
    """Generate a DataFrame with 1-10 random rows matching the given Schema.

    Args:
        schema: A Schema subclass whose ``model_fields`` define the columns.
        partition_column: If set, this column will be filled with *partition_date*
            instead of a random value.
        partition_date: The date to use for the partition column.

    Returns:
        A DataFrame populated with random data matching the schema field types.
    """
    n_rows = random.randint(1, 10)
    rows: list[dict[str, Any]] = []
    for _ in range(n_rows):
        row: dict[str, Any] = {}
        for name, field_info in schema.model_fields.items():
            row[name] = _fake_value(field_info.annotation)
        if partition_column is not None:
            row[partition_column] = partition_date
        rows.append(row)
    return pd.DataFrame(rows)
