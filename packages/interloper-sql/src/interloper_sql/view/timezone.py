"""Per-row timezone conversion for portable views: the day an instant falls on in a named timezone."""

from __future__ import annotations

import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import ibis.expr.types as ir
import sqlglot.expressions as sge
from ibis.backends.sql.compilers.base import SQLGlotCompiler
from ibis.expr import datatypes


class LocalDate(ops.Value):
    """The calendar day an instant falls on in a timezone, which may differ from row to row."""

    instant: ops.Value[datatypes.Timestamp]
    timezone: ops.Value[datatypes.String]

    dtype = datatypes.date
    shape = rlz.shape_like("args")


def _compile_local_date(
    compiler: SQLGlotCompiler, op: LocalDate, *, instant: sge.Expression, timezone: sge.Expression
) -> sge.Expression:
    """Compile a `LocalDate` as sqlglot's timezone conversion, cast to a date.

    Args:
        compiler: The Ibis SQL compiler of the target dialect.
        op: The node being compiled; its arguments arrive already compiled.
        instant: The compiled instant.
        timezone: The compiled IANA timezone name.

    Returns:
        The SQL expression, which sqlglot renders in the compiler's dialect.
    """
    return sge.Cast(this=sge.ConvertTimezone(target_tz=timezone, timestamp=instant), to=sge.DataType.build("date"))


# Ibis converts between zones only by a constant; sqlglot's ConvertTimezone takes a column and renders
# in each dialect's own syntax, so every Ibis SQL compiler learns this node from their common base
SQLGlotCompiler.visit_LocalDate = _compile_local_date  # ty: ignore[unresolved-attribute]


def local_date(instant: ir.TimestampValue, timezone: ir.StringValue | str) -> ir.DateValue:
    """The calendar day *instant* falls on in *timezone*: 22:00 UTC on 4 May is 5 May in Berlin.

    A vendor that schedules in UTC instants still means the account's own
    days; this reads them back. The conversion renders on BigQuery, DuckDB,
    Postgres, Snowflake and Databricks. T-SQL names its zones the Windows way
    and ClickHouse converts only by a constant zone, so a view using this does
    not run there.

    Args:
        instant: A UTC instant column.
        timezone: An IANA timezone name, per row as a column or the same for every row; NULL gives a NULL day.

    Returns:
        A date column.
    """
    return LocalDate(instant, timezone).to_expr()  # ty: ignore[invalid-argument-type]
