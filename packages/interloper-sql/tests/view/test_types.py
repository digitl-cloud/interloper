"""Tests for the schema and dialect mappings."""

from __future__ import annotations

import datetime as dt

import ibis
import interloper as il

from interloper_sql.view.types import IBIS_DIALECTS, ibis_schema


class Rows(il.Schema):
    id: str
    count: int | None
    amount: float | None
    flag: bool | None
    day: dt.date | None
    at: dt.datetime | None


def test_every_field_type_maps_to_an_ibis_type():
    assert ibis_schema(Rows) == ibis.schema(
        {
            "id": "string",
            "count": "int64",
            "amount": "float64",
            "flag": "boolean",
            "day": "date",
            "at": "timestamp('UTC')",
        }
    )


def test_every_warehouse_dialect_has_an_ibis_name():
    assert IBIS_DIALECTS == {
        "bigquery": "bigquery",
        "duckdb": "duckdb",
        "snowflake": "snowflake",
        "clickhouse": "clickhouse",
        "databricks": "databricks",
        "tsql": "mssql",
        "postgres": "postgres",
    }
