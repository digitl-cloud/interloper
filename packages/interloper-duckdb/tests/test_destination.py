"""Tests for ``interloper_duckdb.destination``, against a real DuckDB file."""

import datetime
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any

import interloper as il
import pandas as pd
import pytest
from interloper.destination import IOContext
from interloper.errors import DataNotFoundError
from interloper.partitioning import Partition, PartitionConfig, TimePartition, TimePartitionWindow
from interloper.schema import Schema
from pydantic import BaseModel

from interloper_duckdb import DuckDBConnection, DuckDBDestination


class Point(BaseModel):
    x: int | None = None


class Everything(Schema):
    flag: bool | None
    count: int | None
    ratio: float | None
    amount: Decimal | None
    at: datetime.datetime | None
    day: datetime.date | None
    blob: bytes | None
    label: str | None
    anything: Any = None
    point: Point | None = None
    tags: list[str] | None = None


class DayRow(Schema):
    day: datetime.date | None
    value: int | None


class RegionRow(Schema):
    region: str | None
    value: int | None


@il.asset
def plain() -> list:
    return []


@il.asset(dataset="sales")
def in_sales() -> list:
    return []


@il.asset(partitioning=il.TimePartitionConfig(column="day"))
def daily() -> list:
    return []


@il.asset(partitioning=PartitionConfig(column="region"))
def regional() -> list:
    return []


def _ctx(asset: Any, scope: Any = None, schema: type[Schema] | None = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


def _query(destination: DuckDBDestination, sql: str, parameters: list | None = None) -> list[tuple]:
    cursor = destination.connection.client.cursor()
    try:
        return cursor.execute(sql, parameters or []).fetchall()
    finally:
        cursor.close()


def _column_types(destination: DuckDBDestination, schema: str, table: str) -> dict[str, str]:
    rows = _query(
        destination,
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, table],
    )
    return dict(rows)


def _days(start: int, end: int) -> list[dict[str, Any]]:
    return [{"day": datetime.date(2024, 1, d), "value": d} for d in range(start, end + 1)]


class TestPlacement:
    def test_asset_without_dataset_lands_in_main(self, destination):
        destination.write(_ctx(plain()), [{"a": 1}])
        assert _query(destination, 'SELECT * FROM "main"."plain"') == [(1,)]

    def test_default_dataset_is_the_fallback(self, connection):
        destination = DuckDBDestination(id="d", connection=connection, default_dataset="staging")
        destination.write(_ctx(plain()), [{"a": 1}])
        assert _query(destination, 'SELECT * FROM "staging"."plain"') == [(1,)]

    def test_asset_dataset_wins_over_the_default(self, connection):
        destination = DuckDBDestination(id="d", connection=connection, default_dataset="staging")
        destination.write(_ctx(in_sales()), [{"a": 1}])
        assert _query(destination, 'SELECT * FROM "sales"."in_sales"') == [(1,)]

    def test_in_memory_database_is_shared_across_cursors(self):
        destination = DuckDBDestination(id="d", connection=DuckDBConnection(id="c", database=":memory:"))
        destination.write(_ctx(plain()), [{"a": 1}])
        assert destination.read(_ctx(plain()))["a"].tolist() == [1]


class TestDDL:
    def test_every_type_maps_to_its_column(self, destination):
        row = {
            "flag": True,
            "count": 1,
            "ratio": 0.5,
            "amount": Decimal("1.25"),
            "at": datetime.datetime(2024, 1, 1, 3),
            "day": datetime.date(2024, 1, 1),
            "blob": b"x",
            "label": "a",
            "anything": "free",
            "point": {"x": 1},
            "tags": ["a", "b"],
        }
        destination.write(_ctx(plain(), schema=Everything), [row])

        assert _column_types(destination, "main", "plain") == {
            "flag": "BOOLEAN",
            "count": "BIGINT",
            "ratio": "DOUBLE",
            "amount": "DECIMAL(38,9)",
            "at": "TIMESTAMP",
            "day": "DATE",
            "blob": "BLOB",
            "label": "VARCHAR",
            "anything": "VARCHAR",
            "point": "JSON",
            "tags": "JSON",
        }
        stored = _query(destination, 'SELECT "amount", "point", "tags" FROM "main"."plain"')
        assert stored == [(Decimal("1.250000000"), '{"x":1}', '["a","b"]')]

    def test_without_a_schema_the_table_is_typed_from_the_data(self, destination):
        destination.write(_ctx(plain()), [{"n": 1, "day": datetime.date(2024, 1, 1)}])
        assert _column_types(destination, "main", "plain") == {"n": "BIGINT", "day": "DATE"}

    def test_non_nullable_fields_are_not_null(self, destination):
        class Strict(Schema):
            id: int

        destination.write(_ctx(plain(), schema=Strict), [{"id": 1}])
        (nullable,) = _query(
            destination,
            "SELECT is_nullable FROM information_schema.columns WHERE table_name = 'plain' AND column_name = 'id'",
        )
        assert nullable == ("NO",)

    def test_identifiers_are_quoted(self, destination):
        destination.write(_ctx(plain()), [{'odd "name"': 1, "select": 2}])
        assert _column_types(destination, "main", "plain") == {'odd "name"': "BIGINT", "select": "BIGINT"}

    def test_extra_columns_warn_and_are_dropped(self, destination):
        destination.write(_ctx(plain(), schema=DayRow), _days(1, 1))
        rows = [{"day": datetime.date(2024, 1, 2), "value": 2, "surprise": "x"}]
        with pytest.warns(UserWarning, match=r"Columns \['surprise'\] are not in the schema for 'main.plain'"):
            destination.write(_ctx(plain(), schema=DayRow), rows)
        assert _column_types(destination, "main", "plain") == {"day": "DATE", "value": "BIGINT"}
        assert _query(destination, 'SELECT "value" FROM "main"."plain"') == [(2,)]


class TestWrite:
    def test_whole_table_is_replaced(self, destination):
        destination.write(_ctx(plain()), [{"a": 1}, {"a": 2}])
        destination.write(_ctx(plain()), [{"a": 3}])
        assert _query(destination, 'SELECT "a" FROM "main"."plain"') == [(3,)]

    def test_time_partition_replaces_only_its_bounds(self, destination):
        destination.write(
            _ctx(daily(), TimePartitionWindow(datetime.date(2024, 1, 1), datetime.date(2024, 1, 3))), _days(1, 3)
        )
        destination.write(
            _ctx(daily(), TimePartition(datetime.date(2024, 1, 2))),
            [{"day": datetime.date(2024, 1, 2), "value": 20}],
        )
        rows = _query(destination, 'SELECT "day", "value" FROM "main"."daily" ORDER BY "day"')
        assert [value for _, value in rows] == [1, 20, 3]

    def test_monthly_partition_bounds_cover_daily_rows(self, destination):
        @il.asset(partitioning=il.TimePartitionConfig(column="day", granularity=il.TimeGranularity.MONTH))
        def monthly() -> list:
            return []

        destination.write(_ctx(monthly()), [*_days(1, 3), {"day": datetime.date(2024, 2, 1), "value": 99}])
        month = TimePartition(datetime.date(2024, 1, 1), il.TimeGranularity.MONTH)
        destination.write(_ctx(monthly(), month), [{"day": datetime.date(2024, 1, 15), "value": 15}])
        rows = _query(destination, 'SELECT "value" FROM "main"."monthly" ORDER BY "day"')
        assert rows == [(15,), (99,)]

    def test_hourly_bounds_compare_as_iso_on_a_varchar_column(self, destination):
        class HourRow(Schema):
            hour: str | None
            value: int | None

        @il.asset(partitioning=il.TimePartitionConfig(column="hour", granularity=il.TimeGranularity.HOUR))
        def hourly() -> list:
            return []

        rows = [{"hour": f"2024-01-01T{h:02d}:00:00", "value": h} for h in (2, 3, 4)]
        destination.write(_ctx(hourly(), schema=HourRow), rows)
        partition = TimePartition(datetime.datetime(2024, 1, 1, 3), il.TimeGranularity.HOUR)
        destination.write(_ctx(hourly(), partition, HourRow), [{"hour": "2024-01-01T03:30:00", "value": 30}])

        assert _query(destination, 'SELECT "value" FROM "main"."hourly" ORDER BY "hour"') == [(2,), (30,), (4,)]
        assert destination.read(_ctx(hourly(), partition))["value"].tolist() == [30]

    def test_non_time_partition_replaces_by_equality(self, destination):
        rows = [{"region": "eu", "value": 1}, {"region": "us", "value": 2}]
        destination.write(_ctx(regional(), schema=RegionRow), rows)
        destination.write(_ctx(regional(), Partition("eu"), RegionRow), [{"region": "eu", "value": 10}])
        stored = _query(destination, 'SELECT "region", "value" FROM "main"."regional" ORDER BY "region"')
        assert stored == [("eu", 10), ("us", 2)]

    def test_string_id_matches_a_date_column(self, destination):
        @il.asset(partitioning=PartitionConfig(column="day"))
        def by_day_id() -> list:
            return []

        destination.write(_ctx(by_day_id()), _days(1, 2))
        destination.write(_ctx(by_day_id(), Partition("2024-01-01")), [{"day": datetime.date(2024, 1, 1), "value": 10}])
        rows = _query(destination, 'SELECT "value" FROM "main"."by_day_id" ORDER BY "day"')
        assert rows == [(10,), (2,)]

    def test_window_is_written_as_one_batch(self, destination):
        inserts: list[int] = []
        original = DuckDBDestination.insert

        def counting(self, table, dataset, data, context):
            inserts.append(len(data))
            return original(self, table, dataset, data, context)

        destination.write(_ctx(daily()), _days(1, 5))
        window = TimePartitionWindow(datetime.date(2024, 1, 2), datetime.date(2024, 1, 4))
        replacement = [{"day": datetime.date(2024, 1, d), "value": d * 10} for d in (2, 3, 4)]
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(DuckDBDestination, "insert", counting)
            destination.write(_ctx(daily(), window), replacement)

        assert inserts == [3]
        rows = _query(destination, 'SELECT "value" FROM "main"."daily" ORDER BY "day"')
        assert rows == [(1,), (20,), (30,), (40,), (5,)]

    def test_dataframe_input_is_inserted(self, destination):
        frame = pd.DataFrame({"day": [datetime.date(2024, 1, 1)], "value": [7]})
        destination.write(_ctx(plain(), schema=DayRow), frame)
        assert _query(destination, 'SELECT "value" FROM "main"."plain"') == [(7,)]


class TestTransaction:
    def test_a_failed_insert_rolls_the_delete_back(self, destination):
        destination.write(_ctx(daily(), schema=DayRow), _days(1, 2))

        def failing(*args, **kwargs):
            raise RuntimeError("insert failed")

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(DuckDBDestination, "insert", failing)
            with pytest.raises(RuntimeError, match="insert failed"):
                destination.write(_ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), DayRow), _days(1, 1))

        assert _query(destination, 'SELECT "value" FROM "main"."daily" ORDER BY "day"') == [(1,), (2,)]

    def test_a_database_error_in_insert_rolls_back(self, destination):
        class Strict(Schema):
            day: datetime.date | None
            value: int

        destination.write(_ctx(daily(), schema=Strict), _days(1, 2))
        with pytest.raises(Exception, match="NOT NULL"):
            destination.write(
                _ctx(daily(), TimePartition(datetime.date(2024, 1, 1)), Strict),
                [{"day": datetime.date(2024, 1, 1), "value": None}],
            )
        assert _query(destination, 'SELECT "value" FROM "main"."daily" ORDER BY "day"') == [(1,), (2,)]
        assert destination._transactions == {}

    def test_concurrent_writes_to_a_new_schema_all_land(self, connection):
        destination = DuckDBDestination(id="d", connection=connection, default_dataset="fresh")

        def write(n: int) -> None:
            @il.asset(key=f"t{n}")
            def numbered() -> list:
                return []

            destination.write(_ctx(numbered()), [{"n": n}])

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(write, range(8)))

        tables = _query(destination, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fresh'")
        assert len(tables) == 8

    def test_concurrent_partitions_of_a_new_table_all_land(self, destination):
        def write(day: int) -> None:
            destination.write(_ctx(daily(), TimePartition(datetime.date(2024, 1, day))), _days(day, day))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(write, range(1, 9)))

        assert _query(destination, 'SELECT COUNT(*) FROM "main"."daily"') == [(8,)]


class TestRead:
    def test_reads_back_a_dataframe(self, destination):
        destination.write(_ctx(plain(), schema=DayRow), _days(1, 2))
        frame = destination.read(_ctx(plain()))
        assert isinstance(frame, pd.DataFrame)
        assert list(frame.columns) == ["day", "value"]
        assert frame["value"].tolist() == [1, 2]

    def test_reads_one_time_partition(self, destination):
        destination.write(_ctx(daily()), _days(1, 3))
        frame = destination.read(_ctx(daily(), TimePartition(datetime.date(2024, 1, 2))))
        assert frame["value"].tolist() == [2]

    def test_reads_one_non_time_partition(self, destination):
        destination.write(_ctx(regional()), [{"region": "eu", "value": 1}, {"region": "us", "value": 2}])
        frame = destination.read(_ctx(regional(), Partition("us")))
        assert frame["value"].tolist() == [2]

    def test_missing_table_raises(self, destination):
        with pytest.raises(DataNotFoundError, match=r"Table 'main.plain' does not exist. Has the asset been"):
            destination.read(_ctx(plain()))

    def test_delete_on_a_missing_table_is_a_noop(self, destination):
        destination.delete("absent", None, None)


class TestCount:
    def test_counts_rows_per_partition_value(self, destination):
        destination.write(_ctx(daily()), [*_days(1, 2), {"day": datetime.date(2024, 1, 2), "value": 3}])
        assert destination.partition_row_counts(_ctx(daily())) == {"2024-01-01": 1, "2024-01-02": 2}

    def test_missing_table_raises(self, destination):
        with pytest.raises(DataNotFoundError):
            destination.partition_row_counts(_ctx(daily()))
