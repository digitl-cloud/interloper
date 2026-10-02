"""Fabric Warehouse's view of interloper's field types: one table, read in order."""

from __future__ import annotations

import datetime
from decimal import Decimal

from interloper.schema import FieldSpec

# The warehouse has no json column type and recommends varchar instead.
JSON = "varchar(max)"

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_FABRIC: dict[type, str] = {
    bool: "bit",
    int: "bigint",
    float: "float",
    Decimal: "decimal(38,9)",
    datetime.datetime: "datetime2(6)",
    datetime.date: "date",
    bytes: "varbinary(max)",
    str: "varchar(max)",
    dict: JSON,
    list: JSON,
}


def column_type(spec: FieldSpec) -> str:
    """Return the Fabric Warehouse column type for a field spec.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs` or an inferred schema.

    Returns:
        :data:`JSON` for a nested or repeated field, the type the spec's Python
        type maps to otherwise, and ``varchar(max)`` for anything unmapped
        (``typing.Any``).
    """
    if spec.fields is not None or spec.repeated:
        return JSON
    if isinstance(spec.type, type):
        for base, name in _PYTHON_TO_FABRIC.items():
            if issubclass(spec.type, base):
                return name
    return "varchar(max)"
