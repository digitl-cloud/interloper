"""Per-row timezone conversion for portable views: the day an instant falls on in a named timezone."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ibis.expr.types as ir


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
    from interloper_sql.view.nodes import LocalDate

    return LocalDate(instant, timezone).to_expr()  # ty: ignore[invalid-argument-type]
