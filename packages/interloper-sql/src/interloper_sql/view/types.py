"""Ibis's view of interloper's types: schema fields and warehouse dialects, in one place."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import ibis
from interloper.schema import Schema

IBIS_TYPES: dict[Any, str] = {
    str: "string",
    int: "int64",
    float: "float64",
    bool: "boolean",
    dt.date: "date",
    dt.datetime: "timestamp",
    Decimal: "decimal",
    bytes: "binary",
}

IBIS_DIALECTS: dict[str, str] = {
    "bigquery": "bigquery",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "tsql": "mssql",
    "postgres": "postgres",
}


def ibis_schema(schema: type[Schema]) -> ibis.Schema:
    """The Ibis schema of a declared schema, every column nullable.

    A field whose type has no Ibis counterpart (a nested model, ``Any``) is
    typed ``string``: a view selects such a column through, never computes on it.

    Args:
        schema: The declared schema.

    Returns:
        The Ibis schema, in declaration order.
    """
    return ibis.schema({spec.name: IBIS_TYPES.get(spec.type, "string") for spec in schema.field_specs()})
