"""Databricks' view of interloper's field types."""

from __future__ import annotations

import datetime
from decimal import Decimal

from interloper.schema import FieldSpec

VARIANT = "VARIANT"
TIMESTAMP = "TIMESTAMP"

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_DATABRICKS: dict[type, str] = {
    bool: "BOOLEAN",
    int: "BIGINT",
    float: "DOUBLE",
    Decimal: "DECIMAL(38,9)",
    datetime.datetime: TIMESTAMP,
    datetime.date: "DATE",
    bytes: "BINARY",
    str: "STRING",
    dict: VARIANT,
    list: VARIANT,
}

_CLUSTERABLE = frozenset({"BIGINT", "DOUBLE", "DECIMAL(38,9)", TIMESTAMP, "DATE", "STRING"})


def column_type(spec: FieldSpec) -> str:
    """Return the Databricks column type for a field spec.

    A ``datetime`` is a ``TIMESTAMP``, an absolute instant like BigQuery's:
    ``TIMESTAMP_NTZ`` is still in Public Preview and upgrades the Delta table
    protocol. Nested and repeated fields are ``VARIANT``, Databricks' type for
    semi-structured data, queryable with the ``:`` path operator.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs` or an inferred schema.

    Returns:
        ``VARIANT`` for a nested or repeated field, the type the spec's Python
        type maps to otherwise, and ``STRING`` for anything unmapped
        (``typing.Any``).
    """
    if spec.fields is not None or spec.repeated:
        return VARIANT
    if isinstance(spec.type, type):
        for base, name in _PYTHON_TO_DATABRICKS.items():
            if issubclass(spec.type, base):
                return name
    return "STRING"


def clusterable(spec: FieldSpec) -> bool:
    """Whether a field can be a liquid clustering key.

    Args:
        spec: The field spec.

    Returns:
        True when the field's column type is one liquid clustering accepts as a
        key (dates, timestamps, strings and numbers; not booleans, binary or
        ``VARIANT``).
    """
    return column_type(spec) in _CLUSTERABLE
