"""BigQuery's view of interloper's field types: one table, read in both directions.

Column types are emitted in standard SQL names (``INT64``, ``FLOAT64``,
``BOOL``); a table read back from the API reports the legacy aliases
(``INTEGER``, ``FLOAT``, ``BOOLEAN``), which the client's own
``LEGACY_TO_STANDARD_TYPES`` folds back onto the same names, so a query
parameter is always typed in the vocabulary the column was declared in.
"""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from interloper.schema import FieldSpec

if TYPE_CHECKING:
    from google.cloud import bigquery

TIME_PARTITIONABLE = frozenset({"DATE", "DATETIME", "TIMESTAMP"})

# Ordered: the first base class that matches wins, so bool (a subclass of int)
# and datetime (a subclass of date) must come before their parents.
_PYTHON_TO_BIGQUERY: dict[type, str] = {
    bool: "BOOL",
    int: "INT64",
    float: "FLOAT64",
    Decimal: "NUMERIC",
    datetime.datetime: "TIMESTAMP",
    datetime.date: "DATE",
    bytes: "BYTES",
    str: "STRING",
}


def column_type(python_type: Any) -> str:
    """Return the BigQuery column type for a declared Python type.

    Args:
        python_type: The unwrapped type of a :class:`FieldSpec`, or the type of
            a value; anything that is not a class (``typing.Any``) or that
            matches no row of the table is a ``STRING``.

    Returns:
        A standard SQL type name.
    """
    if isinstance(python_type, type):
        for base, name in _PYTHON_TO_BIGQUERY.items():
            if issubclass(python_type, base):
                return name
    return "STRING"


def parameter_type(field_type: str) -> str:
    """Return the query-parameter type for a column type read back from a table.

    Args:
        field_type: A ``SchemaField.field_type``, in legacy or standard spelling.

    Returns:
        The standard SQL type name, ``STRING`` for a spelling the client does not know.
    """
    from google.cloud import bigquery

    standard = bigquery.schema.LEGACY_TO_STANDARD_TYPES.get(field_type)
    return standard.value if standard is not None else "STRING"


def to_fields(specs: Sequence[FieldSpec]) -> list[bigquery.SchemaField]:
    """Map field specs to BigQuery field definitions.

    Args:
        specs: The field specs to map, from :meth:`Schema.field_specs`.

    Returns:
        One ``SchemaField`` per spec, nested models as ``RECORD``, each
        carrying the spec's description when it has one.
    """
    from google.cloud import bigquery

    fields = []
    for spec in specs:
        mode = "REPEATED" if spec.repeated else ("NULLABLE" if spec.nullable else "REQUIRED")
        # SchemaField's description default is a sentinel, not None: only pass it when set.
        described: dict[str, Any] = {"description": spec.description} if spec.description else {}
        if spec.fields is not None:
            fields.append(
                bigquery.SchemaField(spec.name, "RECORD", mode=mode, fields=to_fields(spec.fields), **described)
            )
        else:
            fields.append(bigquery.SchemaField(spec.name, column_type(spec.type), mode=mode, **described))
    return fields
