"""Tests for ``interloper_clickhouse.destination``, run on an embedded chDB engine."""

import datetime
import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any

import interloper as il
import pandas as pd
import pytest
from interloper.destination import IOContext
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning import TimeGranularity
from interloper.partitioning.base import Partition, PartitionConfig
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.schema import Schema
from pydantic import BaseModel, Field

from interloper_clickhouse import ClickHouseConnection, ClickHouseDestination
from interloper_clickhouse import destination as destination_module


class _Nested(BaseModel):
    x: int | None = None


class _AllTypes(Schema):
    flag: bool | None = Field(...)
    n: int = Field(...)
    cost: float | None = Field(...)
    amount: Decimal | None = Field(...)
    at: datetime.datetime | None = Field(...)
    day: datetime.date | None = Field(...)
    blob: bytes | None = Field(...)
    label: str | None = Field(...)
    anything: Any = Field(None)
    nested: _Nested | None = Field(...)
    tags: list[str] = Field(...)


class _DaySchema(Schema):
    day: datetime.date | None = Field(...)
    cost: float | None = Field(...)


class _AtSchema(Schema):
    at: datetime.datetime | None = Field(...)
    cost: float | None = Field(...)


class _RegionSchema(Schema):
    region: str | None = Field(...)
    cost: float | None = Field(...)


@il.asset(dataset="marts")
def ads_stats() -> list:
    return []


@il.asset
def undated() -> list:
    return []


@il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day"))
def daily() -> list:
    return []


@il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day", granularity=TimeGranularity.MONTH))
def monthly() -> list:
    return []


@il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="at", granularity=TimeGranularity.HOUR))
def hourly() -> list:
    return []


@il.asset(dataset="marts", partitioning=PartitionConfig(column="region"))
def regional() -> list:
    return []


def _destination(**overrides: Any) -> ClickHouseDestination:
    connection = ClickHouseConnection(id="ch", host="localhost", password="<password>")
    return ClickHouseDestination(id="dest", connection=connection, **overrides)


def _ctx(asset: Any, scope: Any = None, schema: Any = None) -> IOContext:
    return IOContext(asset=asset, partition_or_window=scope, schema=schema)


def _day(d: int, month: int = 1) -> datetime.date:
    return datetime.date(2024, month, d)


def _rows(clickhouse, sql: str):
    return sorted(clickhouse.rows(sql), key=repr)


def _staging_tables(clickhouse) -> list[tuple]:
    return clickhouse.rows("SELECT database, name FROM system.tables WHERE name LIKE '\\_interloper\\_staging\\_%'")


def _replaces(clickhouse) -> list[str]:
    return [sql for sql in clickhouse.commands if sql.startswith("ALTER TABLE")]


class TestNaming:
    def test_asset_dataset_is_the_database(self, clickhouse):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])

        assert "CREATE DATABASE IF NOT EXISTS `marts`" in clickhouse.commands
        assert clickhouse.rows("SELECT count() FROM marts.ads_stats") == [(1,)]

    def test_default_dataset_fallback(self, clickhouse):
        _destination(default_dataset="raw").write(_ctx(undated(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])

        assert clickhouse.rows("SELECT count() FROM raw.undated") == [(1,)]

    def test_default_database_without_any_dataset(self, clickhouse):
        _destination().write(_ctx(undated(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])

        assert clickhouse.rows("SELECT count() FROM default.undated") == [(1,)]

    def test_identifiers_are_escaped(self, clickhouse):
        assert destination_module._quote("we`ird\\") == "`we\\`ird\\\\`"
        assert destination_module._literal("it's\\") == "'it\\'s\\\\'"

    def test_escaped_identifiers_round_trip(self, clickhouse):
        @il.asset(dataset="we`ird")
        def odd() -> list:
            return []

        _destination().write(_ctx(odd()), [{"a`b": 1}])

        assert _destination().read(_ctx(odd())).to_dict("records") == [{"a`b": 1}]


class TestDDL:
    def test_every_type(self, clickhouse):
        row = dict.fromkeys(_AllTypes.model_fields)
        row["n"] = 1
        row["tags"] = []
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        create = next(s for s in clickhouse.commands if s.startswith("CREATE TABLE IF NOT EXISTS"))
        assert create == (
            "CREATE TABLE IF NOT EXISTS `marts`.`ads_stats` ("
            "`flag` Nullable(Bool), `n` Int64, `cost` Nullable(Float64), `amount` Nullable(Decimal(38, 9)), "
            "`at` Nullable(DateTime64(6, 'UTC')), `day` Nullable(Date32), `blob` Nullable(String), "
            "`label` Nullable(String), `anything` Nullable(String), `nested` Nullable(String), `tags` String"
            ") ENGINE = MergeTree ORDER BY tuple()"
        )

    def test_infers_without_a_schema(self, clickhouse):
        _destination().write(_ctx(ads_stats()), [{"id": 1, "label": "a", "meta": {"k": 1}}])

        create = next(s for s in clickhouse.commands if s.startswith("CREATE TABLE IF NOT EXISTS"))
        assert create == (
            "CREATE TABLE IF NOT EXISTS `marts`.`ads_stats` "
            "(`id` Nullable(Int64), `label` Nullable(String), `meta` Nullable(String)) "
            "ENGINE = MergeTree ORDER BY tuple()"
        )

    def test_partition_column_is_the_key_and_not_nullable(self, clickhouse):
        _destination().write(
            _ctx(monthly(), TimePartition(_day(1), TimeGranularity.MONTH), _DaySchema), [{"day": _day(5), "cost": 1.0}]
        )

        create = next(s for s in clickhouse.commands if s.startswith("CREATE TABLE IF NOT EXISTS"))
        assert create == (
            "CREATE TABLE IF NOT EXISTS `marts`.`monthly` (`day` Date32, `cost` Nullable(Float64)) "
            "ENGINE = MergeTree PARTITION BY toStartOfMonth(`day`) ORDER BY `day`"
        )
        assert clickhouse.rows(
            "SELECT partition_key, sorting_key FROM system.tables WHERE database = 'marts' AND name = 'monthly'"
        ) == [("toStartOfMonth(day)", "day")]

    def test_existing_table_is_not_created(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])
        clickhouse.calls.clear()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 2.0}])

        assert not any(s.startswith(("CREATE DATABASE", "CREATE TABLE IF NOT EXISTS")) for s in clickhouse.commands)

    def test_existing_table_partitioned_differently_raises(self, clickhouse):
        _destination().write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 1.0}])

        @il.asset(
            key="daily",
            dataset="marts",
            partitioning=il.TimePartitionConfig(column="day", granularity=TimeGranularity.MONTH),
        )
        def daily_as_monthly() -> list:
            return []

        with pytest.raises(ConfigError, match=r"is partitioned by 'day', but the asset's partitioning needs"):
            _destination().write(
                _ctx(daily_as_monthly(), TimePartition(_day(1), TimeGranularity.MONTH), _DaySchema),
                [{"day": _day(1), "cost": 1.0}],
            )

    def test_partition_column_missing_from_the_schema_raises(self, clickhouse):
        with (
            pytest.warns(UserWarning, match="Partition column 'day' not found"),
            pytest.raises(ConfigError, match="Partition column 'day' is not in the schema"),
        ):
            _destination().write(_ctx(daily(), TimePartition(_day(1))), [{"cost": 1.0}])

    def test_hourly_on_a_date_column_raises(self, clickhouse):
        @il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day", granularity=TimeGranularity.HOUR))
        def hourly_on_date() -> list:
            return []

        partition = TimePartition(datetime.datetime(2024, 1, 1, 13), TimeGranularity.HOUR)
        with pytest.raises(ConfigError, match="cannot hold hourly partitions"):
            _destination().write(_ctx(hourly_on_date(), partition, _DaySchema), [{"day": _day(1), "cost": 1.0}])


class TestWrite:
    def test_whole_table_replaces_the_single_partition(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])
        clickhouse.calls.clear()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(2), "cost": 2.0}])

        staging = clickhouse.inserts[-1][0]
        assert staging.startswith("`marts`.`_interloper_staging_ads_stats_")
        assert clickhouse.commands == [
            f"CREATE TABLE {staging} AS `marts`.`ads_stats`",
            f"ALTER TABLE `marts`.`ads_stats` REPLACE PARTITION tuple() FROM {staging}",
            f"DROP TABLE IF EXISTS {staging} SYNC",
        ]
        assert clickhouse.rows("SELECT day, cost FROM marts.ads_stats") == [(_day(2), 2.0)]

    def test_time_partition_replaces_only_its_partition(self, clickhouse):
        destination = _destination()
        window = TimePartitionWindow(_day(1), _day(2))
        destination.write(
            _ctx(daily(), window, _DaySchema), [{"day": _day(1), "cost": 1.0}, {"day": _day(2), "cost": 2.0}]
        )
        clickhouse.calls.clear()

        destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 10.0}])

        assert len(_replaces(clickhouse)) == 1
        assert _replaces(clickhouse)[0].startswith("ALTER TABLE `marts`.`daily` REPLACE PARTITION ID '")
        assert _rows(clickhouse, "SELECT day, cost FROM marts.daily") == [(_day(1), 10.0), (_day(2), 2.0)]

    def test_monthly_partition_holds_daily_dates(self, clickhouse):
        destination = _destination()
        january = TimePartition(_day(1), TimeGranularity.MONTH)
        rows = [{"day": _day(d), "cost": float(d)} for d in (3, 17)]
        destination.write(
            _ctx(monthly(), TimePartitionWindow(_day(1), _day(1, 2), TimeGranularity.MONTH), _DaySchema),
            [
                *rows,
                {"day": _day(9, 2), "cost": 9.0},
            ],
        )

        destination.write(_ctx(monthly(), january, _DaySchema), [{"day": _day(20), "cost": 20.0}])

        assert _rows(clickhouse, "SELECT day, cost FROM marts.monthly") == [(_day(20), 20.0), (_day(9, 2), 9.0)]
        assert destination.read(_ctx(monthly(), january)).to_dict("records") == [
            {"day": pd.Timestamp("2024-01-20"), "cost": 20.0}
        ]

    def test_hourly_partitions(self, clickhouse):
        destination = _destination()
        hour = TimePartition(datetime.datetime(2024, 1, 1, 13), TimeGranularity.HOUR)
        destination.write(_ctx(hourly(), hour, _AtSchema), [{"at": datetime.datetime(2024, 1, 1, 13, 20), "cost": 1.0}])
        destination.write(
            _ctx(hourly(), TimePartition(datetime.datetime(2024, 1, 1, 14), TimeGranularity.HOUR), _AtSchema),
            [{"at": datetime.datetime(2024, 1, 1, 14, 5), "cost": 2.0}],
        )
        destination.write(_ctx(hourly(), hour, _AtSchema), [{"at": datetime.datetime(2024, 1, 1, 13, 40), "cost": 3.0}])

        assert _rows(clickhouse, "SELECT toString(at), cost FROM marts.hourly") == [
            ("2024-01-01 13:40:00.000000", 3.0),
            ("2024-01-01 14:05:00.000000", 2.0),
        ]
        assert destination.read(_ctx(hourly(), hour))["cost"].tolist() == [3.0]

    def test_daily_partitions_on_a_datetime_column(self, clickhouse):
        @il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="at"))
        def events() -> list:
            return []

        destination = _destination()
        rows = [{"at": datetime.datetime(2024, 1, d, 23, 59), "cost": float(d)} for d in (1, 2)]
        destination.write(_ctx(events(), TimePartitionWindow(_day(1), _day(2)), _AtSchema), rows)
        destination.write(
            _ctx(events(), TimePartition(_day(1)), _AtSchema),
            [{"at": datetime.datetime(2024, 1, 1, 0, 0), "cost": 9.0}],
        )

        key = clickhouse.rows("SELECT partition_key FROM system.tables WHERE database = 'marts' AND name = 'events'")
        assert key == [("toDate(at)",)]
        assert _rows(clickhouse, "SELECT cost FROM marts.events") == [(2.0,), (9.0,)]

    def test_time_partitions_on_a_text_column(self, clickhouse):
        @il.asset(dataset="marts", partitioning=il.TimePartitionConfig(column="day", granularity=TimeGranularity.MONTH))
        def textual() -> list:
            return []

        destination = _destination()
        destination.write(
            _ctx(textual(), TimePartitionWindow(_day(1), _day(1, 2), TimeGranularity.MONTH)),
            [{"day": "2024-01-15", "cost": 1.0}, {"day": "2024-02-01T10:00:00", "cost": 2.0}],
        )
        destination.write(
            _ctx(textual(), TimePartition(_day(1), TimeGranularity.MONTH)), [{"day": "2024-01-31", "cost": 3.0}]
        )

        assert _rows(clickhouse, "SELECT day, cost FROM marts.textual") == [
            ("2024-01-31", 3.0),
            ("2024-02-01T10:00:00", 2.0),
        ]
        january = destination.read(_ctx(textual(), TimePartition(_day(1), TimeGranularity.MONTH)))
        assert january["day"].tolist() == ["2024-01-31"]

    def test_non_time_partition_replaces_by_value(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(regional(), Partition("eu"), _RegionSchema), [{"region": "eu", "cost": 1.0}])
        destination.write(_ctx(regional(), Partition("us"), _RegionSchema), [{"region": "us", "cost": 2.0}])
        destination.write(_ctx(regional(), Partition("eu"), _RegionSchema), [{"region": "eu", "cost": 3.0}])

        assert _rows(clickhouse, "SELECT region, cost FROM marts.regional") == [("eu", 3.0), ("us", 2.0)]
        assert destination.read(_ctx(regional(), Partition("eu")))["cost"].tolist() == [3.0]

    def test_integer_partition_column(self, clickhouse):
        class _Shard(Schema):
            shard: int = Field(...)
            cost: float | None = Field(...)

        @il.asset(dataset="marts", partitioning=PartitionConfig(column="shard"))
        def sharded() -> list:
            return []

        destination = _destination()
        destination.write(_ctx(sharded(), Partition("1"), _Shard), [{"shard": 1, "cost": 1.0}])
        destination.write(_ctx(sharded(), Partition("2"), _Shard), [{"shard": 2, "cost": 2.0}])
        destination.write(_ctx(sharded(), Partition("1"), _Shard), [{"shard": 1, "cost": 3.0}])

        assert _rows(clickhouse, "SELECT shard, cost FROM marts.sharded") == [(1, 3.0), (2, 2.0)]
        assert destination.read(_ctx(sharded(), Partition("2")))["cost"].tolist() == [2.0]

    def test_window_is_one_batch(self, clickhouse):
        window = TimePartitionWindow(_day(1), _day(3))
        rows = [{"day": _day(d), "cost": float(d)} for d in (1, 2, 3)]
        _destination().write(_ctx(daily(), window, _DaySchema), rows)

        assert len(clickhouse.inserts) == 1
        assert len(clickhouse.inserts[0][1]) == 3
        assert len(_replaces(clickhouse)) == 3
        assert all(" REPLACE PARTITION ID " in sql for sql in _replaces(clickhouse))

    def test_window_clears_the_partitions_its_data_does_not_cover(self, clickhouse):
        destination = _destination()
        window = TimePartitionWindow(_day(1), _day(3))
        destination.write(_ctx(daily(), window, _DaySchema), [{"day": _day(d), "cost": float(d)} for d in (1, 2, 3)])
        clickhouse.calls.clear()

        destination.write(_ctx(daily(), window, _DaySchema), [{"day": _day(1), "cost": 10.0}])

        verbs = [sql.split(" PARTITION ")[0].rsplit(" ", 1)[1] for sql in _replaces(clickhouse)]
        assert sorted(verbs) == ["DROP", "DROP", "REPLACE"]
        assert clickhouse.rows("SELECT day, cost FROM marts.daily") == [(_day(1), 10.0)]

    def test_partition_without_rows_is_dropped(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 1.0}])
        clickhouse.calls.clear()

        destination.write_partition(_ctx(daily(), TimePartition(_day(1)), _DaySchema), TimePartition(_day(1)), [])

        assert len(_replaces(clickhouse)) == 1
        assert " DROP PARTITION ID " in _replaces(clickhouse)[0]
        assert clickhouse.rows("SELECT count() FROM marts.daily") == [(0,)]

    def test_window_over_a_hundred_partitions(self, clickhouse):
        window = TimePartitionWindow(_day(1), datetime.date(2024, 5, 1))
        rows = [{"day": _day(1) + datetime.timedelta(days=n), "cost": 1.0} for n in range(122)]
        _destination().write(_ctx(daily(), window, _DaySchema), rows)

        assert clickhouse.inserts[0][2]["settings"] == {"max_partitions_per_insert_block": 0}
        assert clickhouse.rows("SELECT count() FROM marts.daily") == [(122,)]

    def test_rows_outside_the_partitions_warn_and_are_not_written(self, clickhouse):
        with pytest.warns(UserWarning, match=r"rows in 1 partition\(s\) outside the ones being written"):
            _destination().write(
                _ctx(daily(), TimePartition(_day(1)), _DaySchema),
                [{"day": _day(1), "cost": 1.0}, {"day": _day(5), "cost": 5.0}],
            )

        assert clickhouse.rows("SELECT day FROM marts.daily") == [(_day(1),)]

    def test_empty_data_writes_nothing(self, clickhouse):
        _destination().write(_ctx(ads_stats(), schema=_DaySchema), [])

        assert clickhouse.calls == []

    def test_extra_columns_warn_and_drop(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])
        with pytest.warns(UserWarning, match=r"Columns \['extra'\] are not in the schema"):
            destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0, "extra": "x"}])

        assert list(clickhouse.inserts[-1][1].columns) == ["day", "cost"]


class TestStaging:
    def test_staging_shares_the_target_structure(self, clickhouse):
        clickhouse.failures["ALTER TABLE"] = RuntimeError("stop before the swap")
        with pytest.raises(RuntimeError):
            _destination().write(
                _ctx(monthly(), TimePartition(_day(1), TimeGranularity.MONTH), _DaySchema),
                [{"day": _day(3), "cost": 1.0}],
            )

        create = next(s for s in clickhouse.commands if s.startswith("CREATE TABLE `marts`.`_interloper_staging_"))
        assert create.endswith(" AS `marts`.`monthly`")

    def test_staging_is_dropped_after_a_write(self, clickhouse):
        _destination().write(
            _ctx(daily(), TimePartitionWindow(_day(1), _day(2)), _DaySchema), [{"day": _day(1), "cost": 1.0}]
        )

        assert _staging_tables(clickhouse) == []
        assert clickhouse.commands[-1].startswith("DROP TABLE IF EXISTS `marts`.`_interloper_staging_daily_")

    def test_failed_replace_drops_staging_and_keeps_the_partition(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 1.0}])
        clickhouse.failures["ALTER TABLE"] = RuntimeError("replace failed")

        with pytest.raises(RuntimeError, match="replace failed"):
            destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 2.0}])

        assert _staging_tables(clickhouse) == []
        assert clickhouse.rows("SELECT cost FROM marts.daily") == [(1.0,)]

    def test_failed_load_drops_staging_and_swaps_nothing(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 1.0}])
        clickhouse.failures["INSERT INTO"] = RuntimeError("load failed")
        clickhouse.calls.clear()

        with pytest.raises(RuntimeError, match="load failed"):
            destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 2.0}])

        assert _replaces(clickhouse) == []
        assert _staging_tables(clickhouse) == []
        assert clickhouse.rows("SELECT cost FROM marts.daily") == [(1.0,)]

    def test_each_write_gets_its_own_staging_table(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])

        assert clickhouse.inserts[0][0] != clickhouse.inserts[1][0]


class TestValues:
    def test_every_type_round_trips(self, clickhouse):
        row = {
            "flag": True,
            "n": 1,
            "cost": 1.5,
            "amount": Decimal("1.25"),
            "at": datetime.datetime(2024, 1, 1, 12, 30, 0, 123456),
            "day": _day(1),
            "blob": b"\x00\x01",
            "label": "a",
            "anything": {"k": [1, 2]},
            "nested": {"x": 1},
            "tags": ["a", "b"],
        }
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        stored = clickhouse.rows("SELECT * FROM marts.ads_stats")[0]
        assert stored[:6] == (True, 1, 1.5, Decimal("1.25"), datetime.datetime(2024, 1, 1, 12, 30, 0, 123456), _day(1))
        assert stored[6] == "\x00\x01"
        assert stored[7] == "a"
        assert [json.loads(value) for value in stored[8:]] == [{"k": [1, 2]}, {"x": 1}, ["a", "b"]]

    def test_missing_values_are_null(self, clickhouse):
        row = dict.fromkeys(_AllTypes.model_fields)
        row["n"] = 1
        row["tags"] = []
        _destination().write(_ctx(ads_stats(), schema=_AllTypes), [row])

        nulls = clickhouse.rows(
            "SELECT isNull(flag), isNull(cost), isNull(amount), isNull(at), isNull(day), isNull(blob), "
            "isNull(label), isNull(anything), isNull(nested), tags FROM marts.ads_stats"
        )
        assert nulls == [(True,) * 9 + ("[]",)]

    def test_json_text_is_kept_as_is(self):
        assert destination_module._to_json('{"a": 1}') == '{"a": 1}'
        assert destination_module._to_json(None) is None
        assert destination_module._to_json(float("nan")) is None
        assert destination_module._to_json({"d": _day(1), "v": float("inf")}) == '{"d": "2024-01-01", "v": null}'


class TestRead:
    def test_whole_table(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])

        frame = destination.read(_ctx(ads_stats()))

        assert isinstance(frame, pd.DataFrame)
        assert frame.to_dict("records") == [{"day": pd.Timestamp("2024-01-01"), "cost": 1.0}]
        assert clickhouse.calls[-1] == ("query_df", "SELECT * FROM `marts`.`ads_stats`", None)

    def test_time_partition_by_bounds(self, clickhouse):
        destination = _destination()
        destination.write(
            _ctx(daily(), TimePartitionWindow(_day(1), _day(2)), _DaySchema),
            [
                {"day": _day(1), "cost": 1.0},
                {"day": _day(2), "cost": 2.0},
            ],
        )

        frame = destination.read(_ctx(daily(), TimePartition(_day(1))))

        assert frame["cost"].tolist() == [1.0]
        assert clickhouse.calls[-1] == (
            "query_df",
            "SELECT * FROM `marts`.`daily` WHERE `day` >= {start:Date32} AND `day` < {end:Date32}",
            {"start": _day(1), "end": _day(2)},
        )

    def test_non_time_partition_by_equality(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(regional(), Partition("eu"), _RegionSchema), [{"region": "eu", "cost": 1.0}])

        destination.read(_ctx(regional(), Partition("eu")))

        assert clickhouse.calls[-1] == (
            "query_df",
            "SELECT * FROM `marts`.`regional` WHERE `region` = {value:String}",
            {"value": "eu"},
        )

    def test_window_reads_each_partition(self, clickhouse):
        destination = _destination()
        window = TimePartitionWindow(_day(1), _day(2))
        destination.write(_ctx(daily(), window, _DaySchema), [{"day": _day(d), "cost": float(d)} for d in (1, 2)])

        frames = destination.read(_ctx(daily(), window))

        assert [frame["cost"].tolist() for frame in frames] == [[2.0], [1.0]]

    def test_missing_table_raises(self, clickhouse):
        with pytest.raises(DataNotFoundError, match=r"does not exist\. Has the asset been materialized\?"):
            _destination().read(_ctx(ads_stats()))


class TestCount:
    def test_groups_by_the_partition_column(self, clickhouse):
        destination = _destination()
        rows = [{"day": _day(1), "cost": 1.0}] * 3 + [{"day": _day(2), "cost": 1.0}] * 2
        destination.write(_ctx(daily(), TimePartitionWindow(_day(1), _day(2)), _DaySchema), rows)

        assert destination.partition_row_counts(_ctx(daily())) == {"2024-01-01": 3, "2024-01-02": 2}
        assert clickhouse.calls[-1][1] == (
            "SELECT toString(`day`) AS partition_value, count() AS cnt FROM `marts`.`daily` GROUP BY partition_value"
        )

    def test_missing_table_raises(self, clickhouse):
        with pytest.raises(DataNotFoundError):
            _destination().partition_row_counts(_ctx(daily()))


class TestHooks:
    def test_insert_appends(self, clickhouse):
        destination = _destination()
        context = _ctx(daily(), schema=_DaySchema)
        destination.insert("daily", "marts", [{"day": _day(1), "cost": 1.0}], context)
        destination.insert("daily", "marts", [{"day": _day(1), "cost": 2.0}], context)

        assert _rows(clickhouse, "SELECT cost FROM marts.daily") == [(1.0,), (2.0,)]
        assert _staging_tables(clickhouse) == []

    def test_delete_by_bounds(self, clickhouse):
        destination = _destination()
        destination.write(
            _ctx(daily(), TimePartitionWindow(_day(1), _day(2)), _DaySchema),
            [
                {"day": _day(1), "cost": 1.0},
                {"day": _day(2), "cost": 2.0},
            ],
        )

        destination.delete("daily", "marts", destination._filter(_ctx(daily()), TimePartition(_day(1))))

        assert clickhouse.rows("SELECT cost FROM marts.daily") == [(2.0,)]
        assert clickhouse.calls[-1] == (
            "command",
            "DELETE FROM `marts`.`daily` WHERE `day` >= {start:Date32} AND `day` < {end:Date32}",
            {"start": _day(1), "end": _day(2)},
        )

    def test_delete_everything_truncates(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(ads_stats(), schema=_DaySchema), [{"day": _day(1), "cost": 1.0}])

        destination.delete("ads_stats", "marts", None)

        assert clickhouse.commands[-1] == "TRUNCATE TABLE `marts`.`ads_stats`"
        assert clickhouse.rows("SELECT count() FROM marts.ads_stats") == [(0,)]

    def test_delete_on_a_missing_table_does_nothing(self, clickhouse):
        _destination().delete("ads_stats", "marts", None)

        assert clickhouse.commands == []

    def test_text_bounds_are_iso(self):
        predicate, parameters = destination_module._predicate(
            destination_module.PartitionFilter(
                "day", bounds=(datetime.datetime(2024, 1, 1, 13), datetime.datetime(2024, 1, 1, 14))
            ),
            {"day": "String"},
        )

        assert predicate == "`day` >= {start:String} AND `day` < {end:String}"
        assert parameters == {"start": "2024-01-01T13:00:00", "end": "2024-01-01T14:00:00"}


class TestWholeWriteOfAPartitionedAsset:
    def test_replaces_every_partition(self, clickhouse):
        destination = _destination()
        window = TimePartitionWindow(_day(1), _day(2))
        destination.write(_ctx(daily(), window, _DaySchema), [{"day": _day(d), "cost": float(d)} for d in (1, 2)])

        destination.write(_ctx(daily(), schema=_DaySchema), [{"day": _day(3), "cost": 3.0}])

        assert clickhouse.rows("SELECT day, cost FROM marts.daily") == [(_day(3), 3.0)]
        assert _staging_tables(clickhouse) == []


class TestConcurrency:
    def test_one_destination_writes_partitions_from_several_threads(self, clickhouse):
        destination = _destination()
        destination.write(_ctx(daily(), TimePartition(_day(1)), _DaySchema), [{"day": _day(1), "cost": 0.0}])

        def write(day: int) -> None:
            destination.write(
                _ctx(daily(), TimePartition(_day(day)), _DaySchema), [{"day": _day(day), "cost": float(day)}]
            )

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(write, range(1, 9)))

        assert _rows(clickhouse, "SELECT cost FROM marts.daily") == [(float(d),) for d in range(1, 9)]
        assert _staging_tables(clickhouse) == []
