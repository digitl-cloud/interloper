"""ClickHouse's view of interloper's field types."""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

from interloper.schema import FieldSpec

STRING = "String"

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_CLICKHOUSE: dict[type, str] = {
    bool: "Bool",
    int: "Int64",
    float: "Float64",
    Decimal: "Decimal(38, 9)",
    datetime.datetime: "DateTime64(6, 'UTC')",
    datetime.date: "Date32",
    bytes: STRING,
    str: STRING,
}


def is_json(spec: FieldSpec) -> bool:
    """Return whether a field is stored as JSON text in a ``String`` column.

    Nested models, repeated fields, plain ``dict``/``list`` fields and fields
    of no concrete class (``typing.Any``) have no scalar ClickHouse type.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs` or an inferred schema.

    Returns:
        True when the field's values are written as JSON text.
    """
    if spec.fields is not None or spec.repeated or spec.type is Any or not isinstance(spec.type, type):
        return True
    return issubclass(spec.type, (dict, list))


def is_nullable(spec: FieldSpec) -> bool:
    """Return whether a field's column is ``Nullable``.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs` or an inferred schema.

    Returns:
        True for a nullable field, and for a ``typing.Any`` field, which admits ``None``.
    """
    return spec.nullable or spec.type is Any


def column_type(spec: FieldSpec) -> str:
    """Return the ClickHouse column type for a field spec, before nullability.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs` or an inferred schema.

    Returns:
        ``String`` for a field stored as JSON text (see :func:`is_json`), the
        type the spec's Python type maps to otherwise, and ``String`` for any
        class that maps to none.
    """
    if is_json(spec):
        return STRING
    for base, name in _PYTHON_TO_CLICKHOUSE.items():
        if issubclass(spec.type, base):
            return name
    return STRING


def base_type(type_name: str) -> str:
    """Strip a ``Nullable`` wrapper from a ClickHouse type name.

    Args:
        type_name: A ClickHouse type name, as ``system.columns`` reports it.

    Returns:
        The wrapped type for ``Nullable(T)``, the name unchanged otherwise.
    """
    if type_name.startswith("Nullable(") and type_name.endswith(")"):
        return type_name[len("Nullable(") : -1]
    return type_name
