"""Tests for local_date: the day an instant falls on, in a timezone named per row."""

from __future__ import annotations

import datetime as dt

import ibis
import pytest

from interloper_sql import local_date

INSTANTS = ibis.table({"instant": "timestamp('UTC')", "timezone": "string"}, name="instants")
DAYS = INSTANTS.select(day=local_date(INSTANTS["instant"], INSTANTS["timezone"]))


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("bigquery", "CAST(TIMESTAMP(DATETIME(`t0`.`instant`, `t0`.`timezone`)) AS DATE)"),
        ("duckdb", 'CAST("t0"."instant" AT TIME ZONE "t0"."timezone" AS DATE)'),
        ("postgres", 'CAST("t0"."instant" AT TIME ZONE "t0"."timezone" AS DATE)'),
        ("snowflake", 'CAST(CONVERT_TIMEZONE("t0"."timezone", "t0"."instant") AS DATE)'),
        ("databricks", "CAST(CONVERT_TIMEZONE(`t0`.`timezone`, `t0`.`instant`) AS DATE)"),
    ],
)
def test_the_conversion_renders_in_each_dialects_own_syntax(dialect: str, expected: str):
    assert expected in ibis.to_sql(DAYS, dialect=dialect)


def test_the_day_is_read_in_each_rows_own_timezone_on_duckdb():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(
        "CREATE TABLE instants AS SELECT * FROM (VALUES "
        "(TIMESTAMPTZ '2026-05-04 22:00:00+00', 'Europe/Berlin'), "
        "(TIMESTAMPTZ '2026-05-04 03:00:00+00', 'America/Los_Angeles'), "
        "(TIMESTAMPTZ '2026-05-04 12:00:00+00', NULL)"
        ") AS t(instant, timezone)"
    )
    rows = connection.execute(str(ibis.to_sql(DAYS, dialect="duckdb"))).fetchall()
    assert [day for (day,) in rows] == [dt.date(2026, 5, 5), dt.date(2026, 5, 3), None]


def test_a_timezone_may_be_a_constant():
    sql = ibis.to_sql(INSTANTS.select(day=local_date(INSTANTS["instant"], "Europe/Berlin")), dialect="duckdb")
    assert "AT TIME ZONE 'Europe/Berlin'" in sql
