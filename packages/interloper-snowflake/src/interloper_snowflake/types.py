"""Snowflake's view of interloper's field types."""

from __future__ import annotations

import datetime
from decimal import Decimal

from interloper.schema import FieldSpec

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_SNOWFLAKE: dict[type, str] = {
    bool: "BOOLEAN",
    int: "NUMBER(38,0)",
    float: "FLOAT",
    Decimal: "NUMBER(38,9)",
    datetime.datetime: "TIMESTAMP_NTZ",
    datetime.date: "DATE",
    bytes: "BINARY",
    str: "VARCHAR",
    dict: "VARIANT",
    list: "VARIANT",
}


def column_type(spec: FieldSpec) -> str:
    """Return the Snowflake column type for a field spec.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs` or an inferred schema.

    Returns:
        ``VARIANT`` for a nested or repeated field, the type the spec's Python
        type maps to otherwise, and ``VARCHAR`` for anything unmapped
        (``typing.Any``).
    """
    if spec.fields is not None or spec.repeated:
        return "VARIANT"
    if isinstance(spec.type, type):
        for base, name in _PYTHON_TO_SNOWFLAKE.items():
            if issubclass(spec.type, base):
                return name
    return "VARCHAR"
