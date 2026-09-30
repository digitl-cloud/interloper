"""DuckDB's view of interloper's field types."""

from __future__ import annotations

import datetime
from decimal import Decimal

from interloper.schema import FieldSpec

JSON = "JSON"

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_DUCKDB: dict[type, str] = {
    bool: "BOOLEAN",
    int: "BIGINT",
    float: "DOUBLE",
    Decimal: "DECIMAL(38,9)",
    datetime.datetime: "TIMESTAMP",
    datetime.date: "DATE",
    bytes: "BLOB",
    str: "VARCHAR",
}


def column_type(spec: FieldSpec) -> str:
    """Return the DuckDB column type for a field spec.

    Nested models and repeated fields are stored as ``JSON``; a scalar maps
    through its Python type, and anything that is not a class
    (``typing.Any``) or that matches no known type is a ``VARCHAR``.

    Args:
        spec: The field spec, from :meth:`Schema.field_specs`.

    Returns:
        A DuckDB type name.
    """
    if spec.fields is not None or spec.repeated:
        return JSON
    if isinstance(spec.type, type):
        for base, name in _PYTHON_TO_DUCKDB.items():
            if issubclass(spec.type, base):
                return name
    return "VARCHAR"
