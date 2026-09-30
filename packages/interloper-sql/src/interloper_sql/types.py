"""SQLAlchemy's view of interloper's field types: one table, read in order."""

from __future__ import annotations

import datetime
from decimal import Decimal

from interloper.schema import FieldSpec
from sqlalchemy import JSON, BigInteger, Boolean, Date, DateTime, Double, LargeBinary, Numeric, Text
from sqlalchemy.types import TypeEngine

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_SQL: dict[type, TypeEngine] = {
    bool: Boolean(),
    int: BigInteger(),
    float: Double(),
    Decimal: Numeric(38, 9),
    datetime.datetime: DateTime(timezone=True),
    datetime.date: Date(),
    bytes: LargeBinary(),
    str: Text(),
}


def column_type(spec: FieldSpec) -> TypeEngine:
    """Return the SQLAlchemy column type for a field spec.

    Args:
        spec: The field spec to map, from :meth:`Schema.field_specs`. A nested
            model or a repeated field is ``JSON``; a type that is not a class
            (``typing.Any``) or matches no row of the table is ``Text``.

    Returns:
        The column type, rendered by each dialect in its own vocabulary.
    """
    if spec.fields is not None or spec.repeated:
        return JSON()
    if isinstance(spec.type, type):
        for base, sql_type in _PYTHON_TO_SQL.items():
            if issubclass(spec.type, base):
                return sql_type
    return Text()
