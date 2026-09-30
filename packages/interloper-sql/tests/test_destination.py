"""Tests for ``interloper_sql.destination``, against a real SQLite database."""

import copy
import datetime
import math
import threading
from typing import Any

import interloper as il
import pytest
import sqlalchemy
from interloper.destination import IOContext
from interloper.errors import DataNotFoundError
from interloper.partitioning.base import Partition, PartitionConfig
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.schema import Schema
from pydantic import Field
from sqlalchemy import Column, Date, DateTime, MetaData, Table, Text, event, exc
from sqlalchemy.types import NullType

from interloper_sql import SQLDestination
from interloper_sql.destination import _coerce


class _DayRow(Schema):
    day: datetime.date | None = Field(...)
    value: int | None = Field(...)


class _RegionRow(Schema):
    region: str | None = Field(...)
    value: int | None = Field(...)


@il.asset
def plain_asset() -> list:
    return []


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def daily_asset(context: il.ExecutionContext) -> list:
    return []


@il.asset(partitioning=PartitionConfig(column="region"))
def regional_asset(context: il.ExecutionContext) -> list:
    return []


def _ctx(asset: Any, scope: Any = None, schema: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


def _day(n: int) -> datetime.date:
    return datetime.date(2024, 1, n)


def _statements(destination: SQLDestination, verb: str) -> list[tuple[str, int]]:
    """Record each executed statement starting with *verb*, with its row count.

    Returns:
        The list the listener appends ``(statement, rows)`` to.
    """
    recorded: list[tuple[str, int]] = []

    @event.listens_for(destination.connection.engine, "before_cursor_execute")
    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith(verb):
            recorded.append((statement, len(parameters) if executemany else 1))

    return recorded


class TestResolveDataset:
    def test_asset_dataset_wins(self, connection):
        destination = SQLDestination(id="d", connection=connection, default_dataset="fallback")
        assert destination._resolve_dataset("explicit") == "explicit"

    def test_falls_back_to_default_dataset(self, connection):
        destination = SQLDestination(id="d", connection=connection, default_dataset="fallback")
        assert destination._resolve_dataset(None) == "fallback"

    def test_empty_string_falls_back(self, connection):
        destination = SQLDestination(id="d", connection=connection, default_dataset="fallback")
        assert destination._resolve_dataset("") == "fallback"

    def test_none_is_the_default_schema(self, destination):
        assert destination._resolve_dataset(None) is None

    def test_asset_dataset_names_the_schema(self, destination):
        destination.write(_ctx(plain_asset()(dataset="main")), [{"a": 1}])

        assert sqlalchemy.inspect(destination.connection.engine).has_table("plain_asset", schema="main")
        assert destination.read(_ctx(plain_asset()(dataset="main"))) == [{"a": 1}]

    def test_default_dataset_names_the_schema(self, connection):
        destination = SQLDestination(id="d", connection=connection, default_dataset="main")
        destination.write(_ctx(plain_asset()), [{"a": 1}])

        assert sqlalchemy.inspect(connection.engine).has_table("plain_asset", schema="main")


class TestNeedsSchema:
    def test_no_dataset_needs_none(self, connection):
        with connection.engine.connect() as conn:
            assert SQLDestination._needs_schema(None, conn) is False

    def test_existing_schema_is_not_created(self, connection):
        with connection.engine.connect() as conn:
            assert SQLDestination._needs_schema("main", conn) is False

    def test_missing_schema_is_created(self, connection):
        with connection.engine.connect() as conn:
            assert SQLDestination._needs_schema("analytics", conn) is True

    def test_dialect_without_schemas_creates_none(self, connection):
        connection.engine.dialect.supports_schemas = False
        with connection.engine.connect() as conn:
            assert SQLDestination._needs_schema("analytics", conn) is False


class TestWholeTable:
    def test_round_trip(self, destination):
        rows = [{"id": 1, "label": "a"}, {"id": 2, "label": None}]
        destination.write(_ctx(plain_asset()), rows)

        assert destination.read(_ctx(plain_asset())) == rows

    def test_rewrite_replaces_every_row(self, destination):
        destination.write(_ctx(plain_asset()), [{"id": 1}, {"id": 2}])
        destination.write(_ctx(plain_asset()), [{"id": 3}])

        assert destination.read(_ctx(plain_asset())) == [{"id": 3}]

    def test_table_is_typed_from_the_schema(self, destination):
        destination.write(_ctx(daily_asset(), None, _DayRow), [{"day": _day(1), "value": 1}])

        columns = {
            c["name"]: c["type"] for c in sqlalchemy.inspect(destination.connection.engine).get_columns("daily_asset")
        }
        assert isinstance(columns["day"], Date)
        assert isinstance(columns["value"], sqlalchemy.BigInteger)

    def test_table_is_typed_from_inferred_schema_without_one(self, destination):
        destination.write(_ctx(plain_asset()), [{"n": 1, "day": _day(1)}])

        columns = {
            c["name"]: c["type"] for c in sqlalchemy.inspect(destination.connection.engine).get_columns("plain_asset")
        }
        assert isinstance(columns["n"], sqlalchemy.BigInteger)
        assert isinstance(columns["day"], Date)

    def test_non_finite_floats_are_null(self, destination):
        destination.write(_ctx(plain_asset()), [{"x": math.nan}, {"x": math.inf}, {"x": 1.5}])

        assert destination.read(_ctx(plain_asset())) == [{"x": None}, {"x": None}, {"x": 1.5}]

    def test_inserts_in_batches_of_a_thousand(self, destination):
        inserts = _statements(destination, "INSERT")
        destination.write(_ctx(plain_asset()), [{"i": i} for i in range(2500)])

        assert [count for _, count in inserts] == [1000, 1000, 500]


class TestTimePartition:
    def test_replaces_only_its_bounds(self, destination):
        destination.write(
            _ctx(daily_asset(), None, _DayRow), [{"day": _day(1), "value": 1}, {"day": _day(2), "value": 2}]
        )
        destination.write(_ctx(daily_asset(), TimePartition(_day(1)), _DayRow), [{"day": _day(1), "value": 10}])

        rows = destination.read(_ctx(daily_asset()))
        assert sorted(rows, key=lambda r: r["day"]) == [{"day": _day(1), "value": 10}, {"day": _day(2), "value": 2}]

    def test_reads_by_partition(self, destination):
        destination.write(
            _ctx(daily_asset(), None, _DayRow), [{"day": _day(1), "value": 1}, {"day": _day(2), "value": 2}]
        )

        assert destination.read(_ctx(daily_asset(), TimePartition(_day(2)))) == [{"day": _day(2), "value": 2}]

    def test_monthly_partition_covers_daily_rows(self, destination):
        @il.asset(partitioning=il.TimePartitionConfig(column="day", granularity=il.TimeGranularity.MONTH))
        def monthly(context: il.ExecutionContext) -> list:
            return []

        rows = [
            {"day": _day(1), "value": 1},
            {"day": _day(31), "value": 2},
            {"day": datetime.date(2024, 2, 1), "value": 3},
        ]
        destination.write(_ctx(monthly(), None, _DayRow), rows)
        partition = TimePartition(_day(15), il.TimeGranularity.MONTH)

        assert len(destination.read(_ctx(monthly(), partition))) == 2


class TestOtherPartition:
    def test_replaces_by_equality(self, destination):
        rows = [{"region": "eu", "value": 1}, {"region": "us", "value": 2}]
        destination.write(_ctx(regional_asset(), None, _RegionRow), rows)
        destination.write(_ctx(regional_asset(), Partition("eu"), _RegionRow), [{"region": "eu", "value": 10}])

        assert destination.read(_ctx(regional_asset(), Partition("eu"))) == [{"region": "eu", "value": 10}]
        assert destination.read(_ctx(regional_asset(), Partition("us"))) == [{"region": "us", "value": 2}]

    def test_id_matches_a_date_column(self, destination):
        @il.asset(partitioning=PartitionConfig(column="day"))
        def by_day_id(context: il.ExecutionContext) -> list:
            return []

        destination.write(
            _ctx(by_day_id(), None, _DayRow), [{"day": _day(1), "value": 1}, {"day": _day(2), "value": 2}]
        )

        assert destination.read(_ctx(by_day_id(), Partition("2024-01-02"))) == [{"day": _day(2), "value": 2}]


class TestWindow:
    def test_writes_one_batch_replacing_each_partition(self, destination):
        destination.write(_ctx(daily_asset(), None, _DayRow), [{"day": _day(n), "value": 0} for n in (1, 2, 3, 4)])
        inserts = _statements(destination, "INSERT")
        deletes = _statements(destination, "DELETE")

        window = TimePartitionWindow(_day(1), _day(3))
        destination.write(_ctx(daily_asset(), window, _DayRow), [{"day": _day(n), "value": n} for n in (1, 2, 3)])

        assert len(deletes) == 3
        assert [count for _, count in inserts] == [3]
        rows = sorted(destination.read(_ctx(daily_asset())), key=lambda r: r["day"])
        assert [r["value"] for r in rows] == [1, 2, 3, 0]


class TestCount:
    def test_groups_by_column_as_strings(self, destination):
        rows = [{"day": _day(1), "value": 1}, {"day": _day(1), "value": 2}, {"day": _day(2), "value": 3}]
        destination.write(_ctx(daily_asset(), None, _DayRow), rows)

        assert destination.partition_row_counts(_ctx(daily_asset())) == {"2024-01-01": 2, "2024-01-02": 1}

    def test_missing_table_raises(self, destination):
        with pytest.raises(DataNotFoundError, match="Has the asset been materialized"):
            destination.partition_row_counts(_ctx(daily_asset()))


class TestMissingTable:
    def test_read_raises(self, destination):
        with pytest.raises(DataNotFoundError, match="Table 'plain_asset' does not exist"):
            destination.read(_ctx(plain_asset()))

    def test_read_names_the_schema(self, destination):
        with pytest.raises(DataNotFoundError, match=r"Table 'main\.plain_asset' does not exist"):
            destination.read(_ctx(plain_asset()(dataset="main")))

    def test_delete_is_a_noop(self, destination):
        destination.delete("never_written", None, None)


class TestExtraColumns:
    def test_warns_and_drops(self, destination):
        destination.write(_ctx(plain_asset(), None, _RegionRow), [{"region": "eu", "value": 1}])

        with pytest.warns(UserWarning, match=r"Columns \['extra'\] are not in the schema for 'plain_asset'"):
            destination.write(_ctx(plain_asset(), None, _RegionRow), [{"region": "eu", "value": 2, "extra": "x"}])

        assert destination.read(_ctx(plain_asset())) == [{"region": "eu", "value": 2}]

    def test_existing_table_is_never_altered(self, destination):
        destination.write(_ctx(plain_asset()), [{"a": 1}])
        with pytest.warns(UserWarning):
            destination.write(_ctx(plain_asset()), [{"a": 2, "b": 3}])

        columns = [c["name"] for c in sqlalchemy.inspect(destination.connection.engine).get_columns("plain_asset")]
        assert columns == ["a"]


class TestTransaction:
    def test_failed_insert_rolls_back_the_delete(self, destination):
        destination.write(_ctx(daily_asset(), None, _DayRow), [{"day": _day(1), "value": 1}])

        # SQLite's Date type refuses a string, so the insert fails after the delete ran.
        with pytest.raises(exc.StatementError):
            destination.write(_ctx(daily_asset(), TimePartition(_day(1)), _DayRow), [{"day": "2024-01-01", "value": 2}])

        assert destination.read(_ctx(daily_asset())) == [{"day": _day(1), "value": 1}]

    def test_hooks_share_the_held_connection(self, destination):
        with destination.transaction():
            held = destination._held[threading.get_ident()]
            with destination._begin() as conn:
                assert conn is held

    def test_held_connection_is_released(self, destination):
        with pytest.raises(RuntimeError), destination.transaction():
            raise RuntimeError("boom")

        assert destination._held == {}

    def test_other_threads_do_not_see_the_held_connection(self, destination):
        seen = []
        with destination.transaction():
            worker = threading.Thread(target=lambda: seen.append(destination._held.get(threading.get_ident())))
            worker.start()
            worker.join()

        assert seen == [None]

    def test_destination_deep_copies(self, destination):
        assert copy.deepcopy(destination).connection.url == destination.connection.url


class TestCoerce:
    @staticmethod
    def _column(sql_type: Any) -> Column:
        return Table("t", MetaData(), Column("c", sql_type)).c.c

    def test_string_to_date(self):
        assert _coerce(self._column(Date()), "2024-01-01") == datetime.date(2024, 1, 1)

    def test_string_to_datetime(self):
        assert _coerce(self._column(DateTime()), "2024-01-01T05:00:00") == datetime.datetime(2024, 1, 1, 5)

    def test_date_to_datetime(self):
        assert _coerce(self._column(DateTime()), _day(1)) == datetime.datetime(2024, 1, 1)

    def test_datetime_is_kept(self):
        value = datetime.datetime(2024, 1, 1, 5)
        assert _coerce(self._column(DateTime()), value) is value

    def test_text_passes_through(self):
        assert _coerce(self._column(Text()), "2024-01-01") == "2024-01-01"

    def test_type_without_python_type_passes_through(self):
        assert _coerce(self._column(NullType()), "x") == "x"
